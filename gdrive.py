#!/usr/bin/python3

import argparse, os, dask, sys, re
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
    
    def query(self, folders=False):
        q = 'trashed = false'
        if self.parent is not None:
            q += f" and '{self.parent}' in parents"
        
        if folders:
            q += f" and mimeType = '{self.folder_mimetype}'"
        elif self.format is not None and not self.recursive:
            q += f" and title contains '{self.format}'"

        return q
    
    def get(self, folders=False):
        opts = {}
        
        if self.shared is not None:
            opts['driveId'] = self.shared
            opts['corpora'] = 'drive'
            opts["includeItemsFromAllDrives"] = True
            opts["supportsAllDrives"] = True
        
        opts['q'] = self.query(folders)
        return opts
    
    def copy(self, parent_folder_obj=None):
        child = deepcopy(self)
        if parent_folder_obj is not None:
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

## download functions

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
    
    file_clone = deepcopy(file_obj)
    try:
        file_clone.GetContentFile(local)
        file_clone = None
    except:
        return

@dask.delayed
def threadDownloads(file_obj, mirror, root_dir):
    downloadFile(file_obj, mirror, root_dir)
    return {'id':file_obj['id'], 'name':file_obj['title']}

## upload functions

def localDirTree(local_dir, format=None):
    sub_dirs = []
    for root, dirs, files in os.walk(local_dir):
        for name in files:
            if format is not None and re.match(f'.*{format}$', name) is None:
                continue            
            temp = os.path.join(root, name)
            temp = re.sub(f"^{local_dir}/*",'',temp)
            sub_dirs.append(temp)
    return sub_dirs

def driveDirTree(drive, opts, local_files):
    keys = list(set([os.path.split(i)[0] for i in local_files]))
    keys = [i for i in keys if i != '']

    id_map = {}

    for k in keys:
        temp_opts = opts.copy()
        fs = getFiles(drive, opts.get(True))
        ft = [f['title'] for f in fs]
        fpath = ''
        for j in k.split(os.path.sep):
            fpath = os.path.join(fpath, j)
            if fpath in id_map:
                obj = deepcopy(id_map[fpath])
            elif j in ft:
                obj = fs[ft.index(j)]              
            else:
                meta = {'title': j, 'mimeType': GlobalOpts.folder_mimetype}
                if temp_opts.parent is not None:
                    meta['parents'] = [{'id':temp_opts.parent}]
                obj = drive.CreateFile(meta)
                obj.Upload({'supportsAllDrives': True})
                fs = []
                ft = []
            
            id_map[fpath] = obj
            temp_opts = temp_opts.copy(obj)
            if not fs == ft == []:
                fs = getFiles(drive, temp_opts.get(True))
                ft = [f['title'] for f in fs]
    
    return id_map

def uploadFile(drive, local_file, parent_hash=None):
    meta = {'uploadType':'resumable'}
    if parent_hash is not None:
        meta['parents'] = [{'id': parent_hash}]
    
    file = drive.CreateFile(meta)
    file.SetContentFile(local_file)    
    file['title'] = os.path.split(local_file)[-1]
    file.Upload({'supportsAllDrives': True})
    return {'id':file['id'], 'name':file['title']}

@dask.delayed
def threadUpload(drive, file, parent_hash=None):    
    return uploadFile(drive, file, parent_hash)    

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
    parser = argparse.ArgumentParser(description="Download or upload entire file directories from/to your google drive on the terminal")    
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
            client = Client()
        else:
            client = Client(processes=False, n_workers=1, threads_per_worker=1)
        
        print(f"dashboard: {client.dashboard_link}")

        procs = [threadDownloads(f, args.mirror, args.directory) for f in files]
        procs = dask.persist(*procs)
        progress(procs)
        client.close()
    
    else:
        if args.recursive:
            files = localDirTree(args.directory, args.format)
        else:
            files = [f for f in os.listdir(args.directory) if os.path.isfile( os.path.join(args.directory,f) )]
            if args.format is not None:
                files = [f for f in files if f.endswith(args.format)]

        if len(files) == 0:
            sys.exit('No files to upload.')

        if args.threaded:
            client = Client()
        else:
            client = Client(processes=False, n_workers=1, threads_per_worker=1)
        
        print(f"dashboard: {client.dashboard_link}")

        procs = []
        if args.mirror:
            folder_map = driveDirTree(drive, opts, files)
            for file in files:
                d,f = os.path.split(file)
                parent_hash = args.parent if d == '' else folder_map[d]['id']
                procs.append( threadUpload(drive, os.path.join(args.directory, file), parent_hash) )
        else:
            for file in files:
                procs.append( threadUpload(drive, os.path.join(args.directory, file), args.parent) )

        procs = dask.persist(*procs)
        progress(procs)
        client.close()
