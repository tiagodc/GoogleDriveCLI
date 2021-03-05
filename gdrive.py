#!/usr/bin/python3

import argparse, os, dask, sys
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from copy import deepcopy
from iteration_utilities import deepflatten
from dask.distributed import Client, progress
import shutil

class GlobalOpts:
    folder_mimetype = 'application/vnd.google-apps.folder'

    def __init__(self, local_dir='.', parent_id=None, shared_id=None, file_format=None, recursive=False):
        self.dir = local_dir
        self.parent = parent_id
        self.shared = shared_id
        self.format = file_format
        self.recursive = recursive
    
    def query(self):
        q = 'trashed = false'
        if self.parent is not None:
            q += f" and '{self.parent}' in parents"
        if self.format is not None and not self.recursive:
            q += f" and title contains '{self.format}'"
        return q
    
    def get(self):
        opts = {}
        
        if self.shared is not None:
            opts['driveId'] = self.shared
            opts['corpora'] = 'drive'
            opts["includeItemsFromAllDrives"] = True
            opts["supportsAllDrives"] = True
        
        opts['q'] = self.query()
        return opts
    
    def copy(self, parent_folder_obj):
        child = deepcopy(self)
        child.parent = parent_folder_obj['id']
        child.dir = os.path.join(child.dir, parent_folder_obj['title'])
        return child

def authenticate(credentials_path = "mycreds.json"):
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_path)

    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()

    gauth.SaveCredentialsFile(credentials_path)

    drive = GoogleDrive(gauth)
    return drive

def getFiles(drive, opts):
    return drive.ListFile(opts).GetList()

def traverseFile(drive, opts, file_obj):
    if file_obj['mimeType'] == GlobalOpts.folder_mimetype and opts.recursive:
        reopts = opts.copy(file_obj)
        f_list = getFiles(drive, reopts.get())
        f_tree = [traverseFile(drive, reopts, f) for f in f_list]
        return f_tree 
    file_obj['local_path'] = os.path.join(opts.dir, file_obj['title'])
    file_obj['local_parent'] = opts.dir
    return file_obj

def getFilesTree(drive, opts):
    files = getFiles(drive, opts.get())
    files = [traverseFile(drive, opts, file_obj=f)  for f in files]
    
    if opts.recursive:
        files = list(deepflatten(files, types=list))
        if opts.format is not None:
            files = [f for f in files if f['title'].endswith(opts.format)]
    else:
        files = [f for f in files if f['mimeType'] != GlobalOpts.folder_mimetype]
            
    return files

def downloadFile(file_obj, mirror=True, root_dir=None):
    if mirror:
        local = file_obj['local_path']
        os.makedirs(file_obj['local_parent'], exist_ok=True)
    else:        
        local = os.path.join(root_dir, file_obj['title'])
    
    try:
        file_obj.GetContentFile(local)
    except:
        return

@dask.delayed
def threadDownloads(file_obj, mirror, root_dir):
    downloadFile(file_obj, mirror, root_dir)
    return file_obj['id']

# def uploadFile(drive, parent_hash, local_file):
#     file = drive.CreateFile({'uploadType':'resumable', 'parents': [{'id': parent_hash}]} )
#     file.SetContentFile(local_file)    
#     file['title'] = local_file.split('/')[-1]
#     file.Upload({'supportsAllDrives': True})
#     return file

# @dask.delayed
# def threadUpload(drive, parent_hash, file):    
#     uploadFile(drive, parent_hash, file)
#     return file    

def str2bool(v):
    if isinstance(v, bool):
       return v
    elif v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download or upload files from/to your google drive from the terminal")    
    parser.add_argument('-d','--directory', required=True, type=str, help='Local directory path')
    parser.add_argument('-p','--parent', required=False, type=str, help="Google Drive folder to use (leave blank to use the drive's root)")   
    parser.add_argument('-s','--shared-drive', required=False, type=str, help='Shared drive ID')
    parser.add_argument('-f','--format', type=str, required=False, help='File format to query')
    parser.add_argument('-l','--list', type=str2bool, default=False, const=True, nargs='?', help='Only print remote files without downloading/uploading')
    parser.add_argument('-u','--upload', type=str2bool, default=False, const=True, nargs='?', help='Upload files from local directory to google drive')
    parser.add_argument('-t','--threaded', type=str2bool, default=False, const=True, nargs='?', help='Use multiple threads (parallel)')
    parser.add_argument('-r','--recursive', type=str2bool, default=False, const=True, nargs='?', help='List files from subdirectories recursively')
    parser.add_argument('-m','--mirror', type=str2bool, default=False, const=True, nargs='?', help='Mirror full paths between google drive and local directory')
    parser.add_argument('-c','--credentials', type=str, default='mycreds.json', help='Path to your credentials file (generated via browser authentication)')
    parser.add_argument('-a','--auth', type=str, default='client_secrets.json', help="Path to your client secrets file (downloaded from the google developer's console)")

    args = parser.parse_args()
    
    if args.auth is not None and args.credentials is None:
        secrets = 'client_secrets.json'
        if not os.path.isfile(secrets):
            shutil.copy(args.auth, secrets)
            drive = authenticate(args.credentials)
            os.unlink(secrets)
    else:
        drive = authenticate(args.credentials)
    
    opts = GlobalOpts(args.directory, args.parent, args.shared_drive, args.format, args.recursive)

    if not args.upload:
        files = getFilesTree(drive, opts)            

        if len(files) == 0:
            sys.exit('No files to download.')

        if args.threaded:
            client = Client(processes=False)
        else:
            client = Client(processes=False, n_workers=1, threads_per_worker=1)
        
        print(f"dashboard: {client.dashboard_link}")

        procs = [threadDownloads(f, args.mirror, args.directory) for f in files]
        procs = dask.persist(*procs)
        progress(procs)
        client.close()
    else:
        print('pending')
