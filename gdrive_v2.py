#!/usr/bin/python3

import argparse
import os
import sys
import re
from typing import Dict, List, Optional
from dataclasses import dataclass
from pathlib import Path
import asyncio
import aiohttp
from google.oauth2.credentials import Credentials
from google.oauth2.authorization import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import pickle

@dataclass
class GlobalOpts:
    folder_mimetype: str = 'application/vnd.google-apps.folder'
    dir: str = '.'
    parent: Optional[str] = None
    shared: Optional[str] = None
    format: Optional[str] = None
    recursive: bool = False

    def query(self, folders: bool = False) -> str:
        q_parts = ['trashed = false']
        if self.parent:
            q_parts.append(f"'{self.parent}' in parents")
        
        if folders:
            q_parts.append(f"mimeType = '{self.folder_mimetype}'")
        elif self.format and not self.recursive:
            q_parts.append(f"name contains '{self.format}'")

        return ' and '.join(q_parts)
    
    def get_params(self, folders: bool = False) -> Dict:
        params = {
            'q': self.query(folders),
            'fields': 'nextPageToken, files(id, name, mimeType, parents)',
            'pageSize': 1000
        }
        
        if self.shared:
            params.update({
                'driveId': self.shared,
                'corpora': 'drive',
                'includeItemsFromAllDrives': True,
                'supportsAllDrives': True
            })
        
        return params

def get_credentials(token_path: str = 'token.pickle', credentials_path: str = 'credentials.json') -> Credentials:
    """Get and refresh Google Drive API credentials."""
    creds = None
    
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path,
                ['https://www.googleapis.com/auth/drive']
            )
            creds = flow.run_local_server(port=0)
        
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    
    return creds

async def list_files(service, opts: GlobalOpts) -> List[Dict]:
    """List files in Google Drive with pagination support."""
    files = []
    page_token = None
    
    try:
        while True:
            params = opts.get_params()
            if page_token:
                params['pageToken'] = page_token
            
            results = service.files().list(**params).execute()
            files.extend(results.get('files', []))
            
            page_token = results.get('nextPageToken')
            if not page_token:
                break
    
    except HttpError as error:
        print(f'An error occurred: {error}')
        files = []
    
    return files

async def traverse_folder(service, opts: GlobalOpts, folder: Dict) -> List[Dict]:
    """Recursively traverse folders and list files."""
    if folder['mimeType'] == GlobalOpts.folder_mimetype and opts.recursive:
        new_opts = GlobalOpts(
            dir=os.path.join(opts.dir, folder['name']),
            parent=folder['id'],
            shared=opts.shared,
            format=opts.format,
            recursive=True
        )
        files = await list_files(service, new_opts)
        tasks = [traverse_folder(service, new_opts, f) for f in files]
        results = await asyncio.gather(*tasks)
        return [item for sublist in results for item in sublist]
    
    folder['local_path'] = os.path.join(opts.dir, folder['name'])
    folder['local_parent'] = opts.dir
    return [folder]

async def download_file(service, file: Dict, mirror: bool = True, root_dir: Optional[str] = None) -> None:
    """Download a single file from Google Drive."""
    if mirror:
        local_path = Path(file['local_path'])
    else:
        local_path = Path(root_dir) / file['name']
    
    local_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        request = service.files().get_media(fileId=file['id'])
        with open(local_path, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
    except HttpError as error:
        print(f'Error downloading {file["name"]}: {error}')

async def upload_file(service, local_file: Path, parent_id: Optional[str] = None) -> Dict:
    """Upload a single file to Google Drive."""
    file_metadata = {
        'name': local_file.name,
        'parents': [parent_id] if parent_id else []
    }
    
    media = MediaFileUpload(
        str(local_file),
        resumable=True
    )
    
    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name',
            supportsAllDrives=True
        ).execute()
        return {'id': file['id'], 'name': file['name']}
    except HttpError as error:
        print(f'Error uploading {local_file}: {error}')
        return {}

async def create_folder_structure(service, opts: GlobalOpts, paths: List[str]) -> Dict[str, Dict]:
    """Create folder structure in Google Drive."""
    folder_map = {}
    unique_paths = set(str(Path(p).parent) for p in paths if p != '')
    
    for path in sorted(unique_paths):
        current_path = Path()
        current_parent = opts.parent or opts.shared
        
        for part in Path(path).parts:
            current_path = current_path / part
            if str(current_path) in folder_map:
                current_parent = folder_map[str(current_path)]['id']
                continue
            
            folder_metadata = {
                'name': part,
                'mimeType': GlobalOpts.folder_mimetype,
                'parents': [current_parent] if current_parent else []
            }
            
            try:
                folder = service.files().create(
                    body=folder_metadata,
                    fields='id,name',
                    supportsAllDrives=True
                ).execute()
                folder_map[str(current_path)] = folder
                current_parent = folder['id']
            except HttpError as error:
                print(f'Error creating folder {part}: {error}')
                break
    
    return folder_map

async def main():
    parser = argparse.ArgumentParser(
        description="Download or upload entire file directories from/to Google Drive"
    )
    # Add argument parsing code here (same as original)
    args = parser.parse_args()
    
    # Initialize the Drive API service
    creds = get_credentials(args.credentials)
    service = build('drive', 'v3', credentials=creds)
    
    opts = GlobalOpts(
        dir=args.directory,
        parent=args.parent,
        shared=args.shared_drive,
        format=args.format,
        recursive=args.recursive
    )
    
    if not args.upload:
        # Download logic
        files = await list_files(service, opts)
        if opts.recursive:
            tasks = [traverse_folder(service, opts, f) for f in files]
            files = [f for sublist in await asyncio.gather(*tasks) for f in sublist]
        
        if not files:
            sys.exit('No files to download.')
        
        if args.list_only:
            print('\nFiles to download:\n')
            for f in files:
                print(f['local_path'])
            sys.exit()
        
        download_tasks = [
            download_file(service, f, args.mirror, args.directory)
            for f in files
        ]
        await asyncio.gather(*download_tasks)
    
    else:
        # Upload logic
        base_path = Path(args.directory)
        if args.recursive:
            files = list(base_path.rglob(f'*{args.format}' if args.format else '*'))
        else:
            files = list(base_path.glob(f'*{args.format}' if args.format else '*'))
        
        files = [f for f in files if f.is_file()]
        
        if not files:
            sys.exit('No files to upload.')
        
        if args.list_only:
            print('\nFiles to upload:\n')
            for f in files:
                print(f)
            sys.exit()
        
        if args.mirror:
            folder_map = await create_folder_structure(
                service, opts,
                [str(f.relative_to(base_path)) for f in files]
            )
            
            upload_tasks = []
            for file in files:
                relative_path = str(file.relative_to(base_path))
                parent_folder = str(Path(relative_path).parent)
                parent_id = (folder_map.get(parent_folder, {}).get('id') 
                           if parent_folder != '.' else (args.parent or args.shared_drive))
                upload_tasks.append(upload_file(service, file, parent_id))
        else:
            upload_tasks = [
                upload_file(service, f, args.parent or args.shared_drive)
                for f in files
            ]
        
        await asyncio.gather(*upload_tasks)

if __name__ == '__main__':
    asyncio.run(main())