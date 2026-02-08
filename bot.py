import os
import json
import asyncio
import threading
import time
import datetime
import io
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait, MessageNotModified
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError
import logging

# Configure logging FIRST before using it
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Import mutagen for audio metadata extraction
try:
    from mutagen import File as MutagenFile
    from mutagen.id3 import ID3
    from mutagen.mp4 import MP4
    from mutagen.flac import FLAC
    MUTAGEN_AVAILABLE = True
except ImportError:
    MUTAGEN_AVAILABLE = False
    logger.warning("⚠️ Mutagen not installed - audio metadata will not be preserved")

# ==================== HEALTH CHECK SERVER ====================
# Ultra-minimal health check (Koyeb-tested and working)
def run_health_server():
    try:
        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
            
            def do_HEAD(self):
                self.send_response(200)
                self.end_headers()
            
            def log_message(self, format, *args):
                pass  # Suppress health check logs
        
        HTTPServer(('0.0.0.0', 8000), HealthHandler).serve_forever()
    except Exception as e:
        logger.error(f"Health server error: {e}")

threading.Thread(target=run_health_server, daemon=True).start()
logger.info("🏥 Health check server starting on port 8000")

# ==================== CONFIGURATION ====================
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
BOT_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID', '')
OWNER_ID = int(os.getenv('OWNER_ID', '0'))
TOKEN_JSON = os.getenv('TOKEN_JSON', '')

# Validate config
if not all([API_ID, API_HASH, BOT_TOKEN, DRIVE_FOLDER_ID, OWNER_ID, TOKEN_JSON]):
    logger.error("❌ Missing environment variables!")
    logger.error(f"API_ID: {bool(API_ID)}, API_HASH: {bool(API_HASH)}, BOT_TOKEN: {bool(BOT_TOKEN)}")
    logger.error(f"DRIVE_FOLDER_ID: {bool(DRIVE_FOLDER_ID)}, OWNER_ID: {bool(OWNER_ID)}, TOKEN_JSON: {bool(TOKEN_JSON)}")
    exit(1)

# Global stats
START_TIME = time.time()
TOTAL_FILES = 0
TOTAL_BYTES = 0

# Initialize Pyrogram client
app = Client(
    "gdrive_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    ipv6=False
)

# Storage for albums and temporary data
ALBUMS = {}
TEMP_FILES = {}
ACTIVE_SERIES = {}
ACTIVE_TASKS = {}  # Track active upload tasks for cancellation

# Queue Management
UPLOAD_QUEUE = {}  # Format: {queue_id: {'files': [], 'series_name': str, 'flat_upload': bool, 'status': str, 'created_at': float}}
QUEUE_COUNTER = 0

# Error Recovery
FAILED_UPLOADS = {}  # Format: {task_id: {'files': [], 'errors': [], 'timestamp': float}}
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# File Browser State
BROWSER_SESSIONS = {}  # Format: {user_id: {'current_folder': str, 'path': [], 'selected_files': [], 'page': int}}
DOWNLOAD_QUEUE = {}  # Format: {task_id: {'files': [], 'status': str, 'progress': int}}

# NEW: Folder selection for uploads
PENDING_FOLDER_SELECTION = {}  # Format: {user_id: {'file_list': [], 'series_name': str, 'mode': str, 'flat_upload': bool}}

# Pagination settings
ITEMS_PER_PAGE = 8

# ==================== GOOGLE DRIVE FUNCTIONS ====================
def get_drive_service():
    """Get authenticated Google Drive service"""
    try:
        creds_data = json.loads(TOKEN_JSON)
        credentials = Credentials.from_authorized_user_info(creds_data)
        service = build('drive', 'v3', credentials=credentials)
        logger.info("✅ Drive service authenticated")
        return service
    except json.JSONDecodeError as e:
        logger.error(f"Invalid TOKEN_JSON format: Failed to parse TOKEN_JSON. Check if it's valid JSON. Error: {e}")
        return None
    except Exception as e:
        logger.error(f"Error creating Drive service: {e}")
        return None

def sync_stats(service, save=False):
    """Sync upload statistics with Google Drive"""
    global TOTAL_FILES, TOTAL_BYTES
    
    if not service:
        return
    
    stats_filename = "bot_stats.json"
    
    try:
        # Search for stats file
        query = f"name='{stats_filename}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false"
        results = service.files().list(
            q=query,
            fields="files(id)"
        ).execute()
        
        files = results.get('files', [])
        file_id = files[0]['id'] if files else None
        
        if save:
            # Save stats to Drive
            stats_data = json.dumps({
                "total_files": TOTAL_FILES,
                "total_bytes": TOTAL_BYTES,
                "last_updated": datetime.datetime.now().isoformat()
            })
            
            media = MediaIoBaseUpload(
                io.BytesIO(stats_data.encode()),
                mimetype='application/json'
            )
            
            if file_id:
                service.files().update(
                    fileId=file_id,
                    media_body=media
                ).execute()
            else:
                service.files().create(
                    body={
                        'name': stats_filename,
                        'parents': [DRIVE_FOLDER_ID]
                    },
                    media_body=media
                ).execute()
            
            logger.info(f"📊 Stats saved: {TOTAL_FILES} files, {TOTAL_BYTES/1024/1024/1024:.2f} GB")
        
        else:
            # Load stats from Drive
            if file_id:
                request = service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                
                stats = json.loads(fh.getvalue().decode())
                TOTAL_FILES = stats.get('total_files', 0)
                TOTAL_BYTES = stats.get('total_bytes', 0)
                
                logger.info(f"📊 Stats loaded: {TOTAL_FILES} files, {TOTAL_BYTES/1024/1024/1024:.2f} GB")
    
    except Exception as e:
        logger.error(f"Error syncing stats: {e}")

def get_or_create_folder(service, folder_name, parent_id, retry_count=0, max_retries=3):
    """
    Find existing folder or create new one with robust error handling
    Includes retry logic and validation to prevent upload failures
    """
    try:
        # Validate inputs
        if not folder_name or not parent_id:
            logger.error(f"Invalid inputs - folder_name: {folder_name}, parent_id: {parent_id}")
            return None
        
        # Sanitize folder name - remove problematic characters
        clean_name = folder_name.replace('"', '\\"').replace('/', '-').replace('\\', '-')
        clean_name = clean_name[:255]  # Drive folder name limit
        
        # Verify parent folder exists
        try:
            service.files().get(fileId=parent_id, fields="id").execute()
        except HttpError as e:
            logger.error(f"Parent folder {parent_id} doesn't exist or is inaccessible: {e}")
            return None
        
        # Search for existing folder with retry
        for attempt in range(max_retries):
            try:
                query = f'name="{clean_name}" and "{parent_id}" in parents and mimeType="application/vnd.google-apps.folder" and trashed=false'
                results = service.files().list(
                    q=query,
                    fields="files(id, name)",
                    pageSize=10
                ).execute()
                
                folders = results.get('files', [])
                
                if folders:
                    # Verify the folder is accessible
                    folder_id = folders[0]['id']
                    try:
                        service.files().get(fileId=folder_id, fields="id,name").execute()
                        logger.info(f"📁 Found existing folder: {folder_name}")
                        return folder_id
                    except HttpError:
                        logger.warning(f"Found folder but couldn't access it, will create new one")
                        break
                else:
                    break
                    
            except HttpError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Search attempt {attempt + 1} failed, retrying...")
                    time.sleep(1 * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"Failed to search for folder after {max_retries} attempts: {e}")
        
        # Create new folder with retry logic
        for attempt in range(max_retries):
            try:
                file_metadata = {
                    'name': clean_name,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [parent_id]
                }
                
                folder = service.files().create(
                    body=file_metadata,
                    fields='id,name'
                ).execute()
                
                folder_id = folder.get('id')
                
                # Verify creation succeeded
                try:
                    service.files().get(fileId=folder_id, fields="id,name").execute()
                    logger.info(f"📁 Created and verified new folder: {folder_name}")
                    return folder_id
                except HttpError as verify_error:
                    logger.error(f"Folder created but verification failed: {verify_error}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    return None
                
            except HttpError as e:
                error_reason = str(e)
                
                # Handle specific error cases
                if 'duplicate' in error_reason.lower():
                    logger.warning(f"Folder '{folder_name}' already exists (race condition), searching again...")
                    time.sleep(1)
                    # Try to find it again
                    try:
                        query = f'name="{clean_name}" and "{parent_id}" in parents and mimeType="application/vnd.google-apps.folder" and trashed=false'
                        results = service.files().list(q=query, fields="files(id, name)", pageSize=10).execute()
                        folders = results.get('files', [])
                        if folders:
                            return folders[0]['id']
                    except:
                        pass
                
                if attempt < max_retries - 1:
                    logger.warning(f"Create attempt {attempt + 1} failed, retrying in {2 * (attempt + 1)}s...")
                    time.sleep(2 * (attempt + 1))
                else:
                    logger.error(f"Failed to create folder '{folder_name}' after {max_retries} attempts: {e}")
                    return None
            
            except Exception as e:
                logger.error(f"Unexpected error creating folder '{folder_name}': {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))
                else:
                    return None
        
        return None
    
    except Exception as e:
        logger.error(f"Critical error in get_or_create_folder for '{folder_name}': {e}")
        return None

def clean_series_name(filename):
    """
    IMPROVED: Extract clean series name from filename
    Removes quality tags, group names, episode info, etc.
    Now with better detection for audiobooks and series
    """
    # Remove file extension
    name = os.path.splitext(filename)[0]
    
    # Store original for fallback
    original = name
    
    # Remove group tags [Group] - but preserve (Book X) patterns
    name = re.sub(r'\[([^\]]+)\]', '', name)  # Remove [...] tags
    name = re.sub(r'\((?!Book\s+\d+)[^\)]+\)', '', name)  # Remove (...) but keep (Book X)
    
    # Remove common release group patterns at end
    name = re.sub(r'-\s*[A-Z]{2,}$', '', name)  # Remove "- RARBG" etc
    
    # Remove quality indicators (comprehensive list)
    quality_patterns = [
        r'\b(1080p|720p|480p|360p|2160p|4K|UHD|FHD|HD|SD)\b',
        r'\b(HDR|HDR10|DolbyVision|DV)\b',
        r'\b(BluRay|BRRip|BDRip|WEBRip|WEBDL|WEB-DL|WEB|DVDRip|HDTV|PDTV)\b',
        r'\b(x264|x265|H\.?264|H\.?265|HEVC|AVC|10bit|8bit)\b',
        r'\b(AAC|AC3|DTS|DD5\.1|DD\+|TrueHD|FLAC|MP3|Atmos)\b',
        r'\b(PROPER|REPACK|RERIP|UNCUT|EXTENDED|THEATRICAL)\b',
    ]
    for pattern in quality_patterns:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
    # Remove episode/chapter/book indicators
    episode_patterns = [
        r'\b(S\d+E\d+|s\d+e\d+)\b',  # S01E01
        r'\b(\d+x\d+)\b',  # 1x01
        r'\b(Episode?\s*\d+|Ep\s*\d+|E\d+)\b',  # Episode 1, Ep1
        r'\b(Ch\s*\d+|Chapter\s*\d+)\b',  # Ch 1, Chapter 1
        r'\b(Part\s*\d+|Pt\s*\d+)\b',  # Part 1
        r'\b(Book\s*\d+)\b',  # Book 1, Book 2
        r'\b(Vol\s*\d+|Volume\s*\d+)\b',  # Vol 1, Volume 1
        r'\b(\d{1,3}\s*of\s*\d{1,3})\b',  # 1 of 12
        r'\b(Disc\s*\d+|CD\s*\d+)\b',  # Disc 1, CD 1
    ]
    for pattern in episode_patterns:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
    # Remove audiobook-specific patterns
    name = re.sub(r'\b(Read\s*by|Narrated\s*by|Narrator)[:\s]+[^-]+', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(Unabridged|Abridged|Retail|Audiobook)\b', '', name, flags=re.IGNORECASE)
    
    # Remove year patterns (4 digits)
    name = re.sub(r'\b(19|20)\d{2}\b', '', name)
    
    # Clean up separators and whitespace
    name = re.sub(r'[._-]+', ' ', name)  # Convert separators to spaces
    name = re.sub(r'\s+', ' ', name).strip()  # Collapse multiple spaces
    
    # Remove trailing/leading special characters
    name = name.strip(' -_.')
    
    # If name is too short after cleaning, try simpler approach
    if len(name) < 3:
        simple = re.sub(r'\b(E\d+|Episode\s*\d+|Ch\s*\d+|Part\s*\d+)\b', '', original, flags=re.IGNORECASE)
        simple = re.sub(r'[._-]+', ' ', simple).strip()
        name = simple if len(simple) >= 3 else original
    
    return name or "Unknown Series"

def clean_filename(filename):
    """
    Clean up filename by replacing underscores with spaces
    Preserves file extension
    """
    # Split filename and extension
    name, ext = os.path.splitext(filename)
    
    # Replace underscores with spaces
    name = name.replace('_', ' ')
    
    # Clean up multiple spaces
    name = re.sub(r'\s+', ' ', name).strip()
    
    # Recombine with extension
    return name + ext

def format_size(bytes_size):
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

def format_time(seconds):
    """Convert seconds to human readable format"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds/60)}m {int(seconds%60)}s"
    else:
        hours = int(seconds/3600)
        mins = int((seconds%3600)/60)
        return f"{hours}h {mins}m"

# ==================== GOOGLE DRIVE DOWNLOAD FUNCTIONS ====================
def extract_file_id_from_url(url):
    """Extract Google Drive file ID from various URL formats"""
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',  # /file/d/FILE_ID
        r'id=([a-zA-Z0-9_-]+)',        # ?id=FILE_ID
        r'/folders/([a-zA-Z0-9_-]+)',  # /folders/FOLDER_ID
        r'/d/([a-zA-Z0-9_-]+)',        # /d/FILE_ID
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None

def get_drive_file_metadata(service, file_id):
    """Get file/folder metadata from Drive - FIXED with better permission handling"""
    try:
        file = service.files().get(
            fileId=file_id,
            fields="id, name, mimeType, size, webViewLink, permissions, capabilities",
            supportsAllDrives=True
        ).execute()
        logger.info(f"✅ File access granted: {file.get('name')}")
        return file
    except HttpError as e:
        error_details = e.error_details[0] if e.error_details else {}
        reason = error_details.get('reason', 'Unknown')
        logger.error(f"❌ HTTP Error accessing file {file_id}: {reason}")
        logger.error(f"Full error: {e}")
        if e.resp.status == 404:
            logger.error("File not found or bot doesn't have access")
        elif e.resp.status == 403:
            logger.error("Permission denied - check if bot email has access")
        return None
    except Exception as e:
        logger.error(f"Error getting file metadata: {e}")
        return None

def list_folder_files_recursive(service, folder_id):
    """List all files in a folder recursively - FIXED with better shared drive support"""
    try:
        all_files = []
        page_token = None
        
        while True:
            query = f"'{folder_id}' in parents and trashed=false"
            results = service.files().list(
                q=query,
                pageSize=1000,
                pageToken=page_token,
                fields="nextPageToken, files(id, name, mimeType, size)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                corpora='allDrives'  # FIXED: Search across all drives including shared
            ).execute()
            
            items = results.get('files', [])
            
            for item in items:
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    # Recursively get files from subfolders
                    subfolder_files = list_folder_files_recursive(service, item['id'])
                    all_files.extend(subfolder_files)
                else:
                    all_files.append(item)
            
            page_token = results.get('nextPageToken')
            if not page_token:
                break
        
        return all_files
    
    except Exception as e:
        logger.error(f"Error listing folder: {e}")
        return []

def download_file_from_drive(service, file_id, file_name):
    """Download file from Google Drive to local storage - FIXED with better permission handling"""
    try:
        # FIXED: First check if we can access the file
        try:
            metadata = service.files().get(
                fileId=file_id,
                fields="id, name, capabilities",
                supportsAllDrives=True
            ).execute()
            
            # FIXED: Check if we have download permission
            capabilities = metadata.get('capabilities', {})
            if not capabilities.get('canDownload', True):
                logger.error(f"❌ No download permission for: {file_name}")
                return None
                
        except HttpError as e:
            logger.error(f"❌ Cannot access file {file_name}: {e}")
            return None
        
        # FIXED: Now download the file with acknowledgeAbuse flag
        request = service.files().get_media(
            fileId=file_id,
            supportsAllDrives=True,
            acknowledgeAbuse=True  # FIXED: Bypass Google's abuse warning if present
        )
        
        download_path = f"downloads/{file_name}"
        os.makedirs("downloads", exist_ok=True)
        
        fh = io.FileIO(download_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request, chunksize=10*1024*1024)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                if progress % 20 == 0:  # Log every 20%
                    logger.info(f"Download {progress}% - {file_name}")
        
        fh.close()
        logger.info(f"✅ Downloaded: {file_name}")
        return download_path
    
    except HttpError as e:
        logger.error(f"HTTP Error downloading {file_name}: {e}")
        logger.error(f"Error details: {e.error_details if hasattr(e, 'error_details') else 'None'}")
        return None
    except Exception as e:
        logger.error(f"Error downloading {file_name}: {e}")
        return None

def add_to_queue(file_list, series_name=None, flat_upload=False):
    """Add upload task to queue"""
    global QUEUE_COUNTER
    QUEUE_COUNTER += 1
    queue_id = f"queue_{QUEUE_COUNTER}"
    
    UPLOAD_QUEUE[queue_id] = {
        'files': file_list,
        'series_name': series_name,
        'flat_upload': flat_upload,
        'status': 'pending',
        'created_at': time.time(),
        'priority': QUEUE_COUNTER
    }
    
    logger.info(f"Added to queue: {queue_id} with {len(file_list)} files")
    return queue_id

def get_queue_status():
    """Get formatted queue status"""
    if not UPLOAD_QUEUE:
        return "📭 Queue is empty"
    
    status_text = "📋 **Upload Queue**\n\n"
    
    for queue_id, data in sorted(UPLOAD_QUEUE.items(), key=lambda x: x[1]['priority']):
        status = data['status']
        files_count = len(data['files'])
        series = data.get('series_name', 'Standalone')
        age = time.time() - data['created_at']
        
        status_emoji = {
            'pending': '⏳',
            'uploading': '🔄',
            'paused': '⏸️',
            'completed': '✅',
            'failed': '❌'
        }.get(status, '❓')
        
        status_text += f"{status_emoji} `{queue_id}`\n"
        status_text += f"  Files: {files_count} | Series: {series}\n"
        status_text += f"  Age: {format_time(age)} | Status: {status}\n\n"
    
    return status_text

def remove_from_queue(queue_id):
    """Remove item from queue"""
    if queue_id in UPLOAD_QUEUE:
        del UPLOAD_QUEUE[queue_id]
        logger.info(f"Removed from queue: {queue_id}")
        return True
    return False

def record_failed_upload(task_id, filename, error):
    """Record failed upload for retry"""
    if task_id not in FAILED_UPLOADS:
        FAILED_UPLOADS[task_id] = {
            'files': [],
            'errors': [],
            'timestamp': time.time()
        }
    
    FAILED_UPLOADS[task_id]['files'].append(filename)
    FAILED_UPLOADS[task_id]['errors'].append(f"{filename}: {str(error)[:100]}")
    logger.error(f"Recorded failed upload: {filename} in task {task_id}")

# ==================== FILE BROWSER FUNCTIONS ====================
def list_drive_files(service, folder_id, page_token=None):
    """List files and folders in a Google Drive folder"""
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        
        results = service.files().list(
            q=query,
            pageSize=100,
            pageToken=page_token,
            fields="nextPageToken, files(id, name, mimeType, size, modifiedTime, parents)",
            orderBy="folder,name"
        ).execute()
        
        items = results.get('files', [])
        next_page_token = results.get('nextPageToken')
        
        # Separate folders and files
        folders = [item for item in items if item['mimeType'] == 'application/vnd.google-apps.folder']
        files = [item for item in items if item['mimeType'] != 'application/vnd.google-apps.folder']
        
        return folders, files, next_page_token
    
    except Exception as e:
        logger.error(f"Error listing Drive files: {e}")
        return [], [], None

def get_folder_info(service, folder_id):
    """Get information about a folder"""
    try:
        if folder_id == DRIVE_FOLDER_ID:
            return {'id': DRIVE_FOLDER_ID, 'name': 'My Drive', 'parents': []}
        
        file = service.files().get(
            fileId=folder_id,
            fields="id, name, parents"
        ).execute()
        
        return file
    
    except Exception as e:
        logger.error(f"Error getting folder info: {e}")
        return None

def search_drive_files(service, query, folder_id=None):
    """Search for files in Google Drive"""
    try:
        search_query = f"name contains '{query}' and trashed=false"
        
        if folder_id:
            search_query += f" and '{folder_id}' in parents"
        
        results = service.files().list(
            q=search_query,
            pageSize=50,
            fields="files(id, name, mimeType, size, modifiedTime, parents)",
            orderBy="folder,name"
        ).execute()
        
        items = results.get('files', [])
        
        # Separate folders and files
        folders = [item for item in items if item['mimeType'] == 'application/vnd.google-apps.folder']
        files = [item for item in items if item['mimeType'] != 'application/vnd.google-apps.folder']
        
        return folders, files
    
    except Exception as e:
        logger.error(f"Error searching Drive: {e}")
        return [], []

def get_browser_session(user_id):
    """Get or create browser session for user"""
    if user_id not in BROWSER_SESSIONS:
        BROWSER_SESSIONS[user_id] = {
            'current_folder': DRIVE_FOLDER_ID,
            'path': [{'name': 'My Drive', 'id': DRIVE_FOLDER_ID}],
            'selected_files': [],
            'page': 0,
            'view_mode': 'browse'  # browse, search, favorites
        }
    return BROWSER_SESSIONS[user_id]

def format_file_item(file_info, selected=False):
    """Format file information for display"""
    name = file_info['name']
    
    # FIXED: Only show size for files, not folders
    mime = file_info.get('mimeType', '')
    is_folder = mime == 'application/vnd.google-apps.folder'
    
    if is_folder:
        size = ''  # No size for folders
    else:
        size = format_size(int(file_info.get('size', 0))) if 'size' in file_info else 'N/A'
    
    # Get file type emoji
    if is_folder:
        emoji = '📁'
    elif 'video' in mime:
        emoji = '🎬'
    elif 'audio' in mime:
        emoji = '🎵'
    elif 'image' in mime:
        emoji = '🖼️'
    elif 'pdf' in mime:
        emoji = '📄'
    elif 'document' in mime or 'text' in mime:
        emoji = '📝'
    else:
        emoji = '📎'
    
    checkbox = '✅' if selected else '☑️'
    
    return f"{emoji} {name[:35]}..." if len(name) > 35 else f"{emoji} {name}", size, checkbox

def build_browser_keyboard(user_id, folders, files, total_items):
    """
    Build inline keyboard for file browser with improved pagination and selection logic
    Features: Better visual feedback, cleaner pagination, organized button layout
    """
    session = get_browser_session(user_id)
    page = session['page']
    selected = set(session['selected_files'])  # Use set for faster lookups
    
    keyboard = []
    
    # Calculate pagination with proper bounds checking
    total_pages = max(1, (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    page = min(page, total_pages - 1)  # Ensure page doesn't exceed bounds
    session['page'] = page  # Update session with validated page
    
    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, total_items)
    
    # Combine folders and files
    all_items = folders + files
    page_items = all_items[start_idx:end_idx]
    
    # Add items with improved visual feedback
    for item in page_items:
        is_folder = item['mimeType'] == 'application/vnd.google-apps.folder'
        is_selected = item['id'] in selected
        
        # Get formatted display name and size (already includes emoji)
        formatted_name, size, _ = format_file_item(item, is_selected)
        
        # Selection checkbox with clear visual states
        if is_selected:
            checkbox = "✅"  # Clear checkmark for selected
        else:
            checkbox = "☐"  # Empty box for unselected
        
        # Truncate name to fit better (name already has emoji from format_file_item)
        display_name = formatted_name[:40] + "..." if len(formatted_name) > 40 else formatted_name
        
        if is_folder:
            # Folders: checkbox + name (with emoji) + open button (no size)
            keyboard.append([
                InlineKeyboardButton(checkbox, callback_data=f"browser_select|{item['id']}"),
                InlineKeyboardButton(display_name, callback_data=f"browser_open|{item['id']}")
            ])
        else:
            # Files: checkbox + name (with emoji) + size
            keyboard.append([
                InlineKeyboardButton(checkbox, callback_data=f"browser_select|{item['id']}"),
                InlineKeyboardButton(display_name, callback_data=f"browser_info|{item['id']}"),
                InlineKeyboardButton(f"💾 {size}", callback_data=f"browser_info|{item['id']}")
            ])
    
    # Pagination row with page numbers
    if total_pages > 1:
        nav_row = []
        
        # Previous button
        if page > 0:
            nav_row.append(InlineKeyboardButton("◀️", callback_data="browser_prev"))
        
        # Page indicator
        nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="browser_noop"))
        
        # Next button
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("▶️", callback_data="browser_next"))
        
        keyboard.append(nav_row)
    
    # Navigation row
    nav_buttons = []
    if len(session['path']) > 1:
        nav_buttons.append(InlineKeyboardButton("⬅️ Back", callback_data="browser_back"))
    nav_buttons.append(InlineKeyboardButton("🏠 Home", callback_data="browser_home"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Selection actions row (only show if items are selected)
    if selected:
        selection_row = []
        selection_count = len(selected)
        
        # Upload to Telegram button - uses browser_upload to handle folders properly
        selection_row.append(
            InlineKeyboardButton(f"📤 Upload to TG ({selection_count})", callback_data="browser_upload")
        )
        selection_row.append(
            InlineKeyboardButton("❌ Clear", callback_data="browser_clear")
        )
        
        keyboard.append(selection_row)
    else:
        # MERGED: Single "Select All" button - selects EVERYTHING (files + folders)
        if total_items > 0:
            keyboard.append([
                InlineKeyboardButton("☑️ Select All", callback_data="browser_select_all")
            ])
    
    # Utility row
    utility_row = []
    utility_row.append(InlineKeyboardButton("🔍 Search", callback_data="browser_search"))
    utility_row.append(InlineKeyboardButton("🔄 Refresh", callback_data="browser_refresh"))
    utility_row.append(InlineKeyboardButton("❌ Close", callback_data="browser_close"))
    
    keyboard.append(utility_row)
    
    return InlineKeyboardMarkup(keyboard)

def build_folder_selection_keyboard(user_id, folders, folder_id):
    """
    Build keyboard for folder selection during upload
    Shows only folders (no files) + select current folder option
    """
    session = BROWSER_SESSIONS.get(user_id, {'page': 0})
    page = session.get('page', 0)
    
    start_idx = page * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    
    # Only show folders for navigation
    page_folders = folders[start_idx:end_idx]
    
    keyboard = []
    
    # Add folder navigation buttons
    for folder in page_folders:
        folder_name = folder['name'][:35]  # Truncate long names
        keyboard.append([InlineKeyboardButton(
            f"📁 {folder_name}", 
            callback_data=f"upload_nav|{folder['id']}"
        )])
    
    # "Select This Folder" button
    keyboard.append([InlineKeyboardButton(
        "✅ Upload Here", 
        callback_data=f"upload_select|{folder_id}"
    )])
    
    # Navigation row (back + pagination)
    nav_row = []
    
    if session.get('path') and len(session['path']) > 1:
        nav_row.append(InlineKeyboardButton("⬅️ Back", callback_data="upload_back"))
    
    # Pagination for folders
    total_pages = (len(folders) - 1) // ITEMS_PER_PAGE + 1 if folders else 1
    current_page = page + 1
    
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️", callback_data="upload_prev"))
    
    if total_pages > 1:
        nav_row.append(InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data="upload_noop"))
    
    if end_idx < len(folders):
        nav_row.append(InlineKeyboardButton("▶️", callback_data="upload_next"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    # Cancel button
    keyboard.append([InlineKeyboardButton("❌ Cancel Upload", callback_data="upload_cancel")])
    
    return InlineKeyboardMarkup(keyboard)

def get_breadcrumb(session):
    """Get breadcrumb navigation path"""
    path_parts = [p['name'] for p in session['path']]
    return " > ".join(path_parts)

async def safe_edit_message(message, text, reply_markup=None):
    """
    Safely edit a message, handling MessageNotModified errors
    """
    try:
        if reply_markup:
            await message.edit_text(text, reply_markup=reply_markup)
        else:
            await message.edit_text(text)
    except MessageNotModified:
        # Message content is the same, silently ignore
        pass
    except Exception as e:
        logger.error(f"Error editing message: {e}")
        raise

def create_progress_bar(progress, length=10):
    """
    Create visual progress bar
    Args:
        progress: Progress percentage (0-100)
        length: Length of progress bar in characters
    Returns:
        String representation of progress bar
    """
    filled = int((progress / 100) * length)
    empty = length - filled
    bar = "■" * filled + "□" * empty
    return f"[{bar}] {progress}%"

def is_audio_or_video_file(filename, mime_type=''):
    """
    Check if file is audio or video (filter out other types like .cue)
    Args:
        filename: Name of the file
        mime_type: MIME type from Google Drive
    Returns:
        True if audio or video, False otherwise
    """
    # Audio extensions
    audio_extensions = (
        '.mp3', '.m4a', '.m4b', '.flac', '.wav', '.ogg', 
        '.aac', '.opus', '.wma', '.ape', '.alac', '.aiff'
    )
    
    # Video extensions
    video_extensions = (
        '.mp4', '.mkv', '.avi', '.mov', '.webm', '.wmv', 
        '.flv', '.3gp', '.m4v', '.mpg', '.mpeg'
    )
    
    filename_lower = filename.lower()
    
    # Check extension
    if filename_lower.endswith(audio_extensions) or filename_lower.endswith(video_extensions):
        return True
    
    # Check MIME type as backup
    if mime_type:
        if 'audio' in mime_type or 'video' in mime_type:
            return True
    
    return False

def extract_audio_metadata(file_path):
    """
    Extract metadata from audio file using mutagen
    Returns dict with title, artist, album, duration, etc.
    """
    if not MUTAGEN_AVAILABLE:
        return {}
    
    try:
        audio = MutagenFile(file_path, easy=True)
        
        if audio is None:
            return {}
        
        metadata = {}
        
        # Extract common tags
        # Title
        if 'title' in audio:
            metadata['title'] = str(audio['title'][0]) if isinstance(audio['title'], list) else str(audio['title'])
        
        # Artist/Performer
        if 'artist' in audio:
            metadata['performer'] = str(audio['artist'][0]) if isinstance(audio['artist'], list) else str(audio['artist'])
        elif 'albumartist' in audio:
            metadata['performer'] = str(audio['albumartist'][0]) if isinstance(audio['albumartist'], list) else str(audio['albumartist'])
        
        # Album
        if 'album' in audio:
            metadata['album'] = str(audio['album'][0]) if isinstance(audio['album'], list) else str(audio['album'])
        
        # Duration
        if hasattr(audio.info, 'length'):
            metadata['duration'] = int(audio.info.length)
        
        logger.info(f"📊 Extracted metadata: {metadata.get('title', 'Unknown')} - {metadata.get('performer', 'Unknown')}")
        return metadata
        
    except Exception as e:
        logger.debug(f"Could not extract metadata from {file_path}: {e}")
        return {}

async def extract_audio_thumbnail(file_path):
    """
    Extract album art/thumbnail from audio file
    Returns path to extracted thumbnail or None
    """
    if not MUTAGEN_AVAILABLE:
        return None
    
    try:
        audio = MutagenFile(file_path)
        
        if audio is None:
            return None
        
        thumb_data = None
        thumb_path = f"{file_path}_thumb.jpg"
        
        # Try to extract cover art based on file type
        if hasattr(audio, 'pictures') and audio.pictures:
            # FLAC, OGG
            thumb_data = audio.pictures[0].data
        elif hasattr(audio, 'tags'):
            # MP3 ID3 tags
            if hasattr(audio.tags, 'getall'):
                pics = audio.tags.getall('APIC')
                if pics:
                    thumb_data = pics[0].data
            # MP4/M4A
            elif 'covr' in audio.tags:
                thumb_data = bytes(audio.tags['covr'][0])
        
        # Save thumbnail if found
        if thumb_data:
            with open(thumb_path, 'wb') as f:
                f.write(thumb_data)
            logger.info(f"🖼️ Extracted thumbnail for {os.path.basename(file_path)}")
            return thumb_path
        
        return None
        
    except Exception as e:
        logger.debug(f"Could not extract thumbnail from {file_path}: {e}")
        return None

async def download_from_drive_task(client, status_msg, file_ids, service):
    """Download files from Google Drive and send to Telegram"""
    task_id = f"download_{status_msg.chat.id}_{status_msg.id}"
    ACTIVE_TASKS[task_id] = {
        'cancelled': False,
        'files_list': file_ids,
        'current_file': None,
        'progress': 0,
        'status': 'initializing'
    }
    
    successful = 0
    failed = 0
    total_files = len(file_ids)
    
    try:
        for idx, file_id in enumerate(file_ids, 1):
            # Check for cancellation
            if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                await status_msg.edit_text(
                    f"🛑 **Download Cancelled**\n\n"
                    f"✅ Downloaded: {successful}/{total_files}\n"
                    f"❌ Failed: {failed}"
                )
                return
            
            try:
                # Get file metadata
                file_metadata = service.files().get(
                    fileId=file_id,
                    fields="id, name, mimeType, size"
                ).execute()
                
                filename = file_metadata['name']
                mime_type = file_metadata.get('mimeType', '')
                file_size = int(file_metadata.get('size', 0))
                
                # FILTER: Skip non-audio/video files
                if not is_audio_or_video_file(filename, mime_type):
                    logger.info(f"⏭️ Skipping non-audio/video file: {filename}")
                    # Don't count as failed, just skip
                    continue
                
                ACTIVE_TASKS[task_id]['current_file'] = filename
                ACTIVE_TASKS[task_id]['progress'] = int((idx - 1) / total_files * 100)
                
                # ADD: Cancel button for downloads
                cancel_button = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🛑 Cancel Download", callback_data=f"cancel_{task_id}")]
                ])
                
                # Update status
                await status_msg.edit_text(
                    f"📥 **Downloading from Drive ({idx}/{total_files})**\n"
                    f"📄 `{filename[:50]}...`\n"
                    f"💾 Size: {format_size(file_size)}\n"
                    f"✅ {successful} | ❌ {failed}",
                    reply_markup=cancel_button
                )
                
                # Download file from Drive
                request = service.files().get_media(fileId=file_id)
                download_path = f"downloads/{filename}"
                
                os.makedirs("downloads", exist_ok=True)
                
                fh = io.FileIO(download_path, 'wb')
                downloader = MediaIoBaseDownload(fh, request)
                
                done = False
                last_update = 0  # Track last update time to avoid FloodWait
                start_time = time.time()  # Track download start time for speed/ETA
                last_progress_bytes = 0  # Track bytes for speed calculation
                last_speed_update = start_time  # Track last speed update time
                
                while not done:
                    # Check for cancellation
                    if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                        fh.close()
                        if os.path.exists(download_path):
                            os.remove(download_path)
                        await status_msg.edit_text(
                            f"🛑 **Download Cancelled**\n\n"
                            f"✅ Downloaded: {successful}/{total_files}\n"
                            f"❌ Failed: {failed}"
                        )
                        return
                    
                    status, done = downloader.next_chunk()
                    
                    # Update progress with visual bar, speed, and ETA
                    if status:
                        progress = int(status.progress() * 100)
                        current_bytes = int(status.progress() * file_size)
                        current_time = time.time()
                        
                        # Only update every 3 seconds to avoid FloodWait
                        if current_time - last_update >= 3 or done:
                            # Calculate speed (bytes per second)
                            time_diff = current_time - last_speed_update
                            bytes_diff = current_bytes - last_progress_bytes
                            
                            if time_diff > 0:
                                speed = bytes_diff / time_diff  # bytes per second
                            else:
                                speed = 0
                            
                            # Calculate ETA
                            if speed > 0 and current_bytes < file_size:
                                remaining_bytes = file_size - current_bytes
                                eta_seconds = remaining_bytes / speed
                                eta_str = format_time(eta_seconds)
                            else:
                                eta_str = "Calculating..." if current_bytes < file_size else "Done"
                            
                            # Update tracking variables
                            last_update = current_time
                            last_speed_update = current_time
                            last_progress_bytes = current_bytes
                            
                            progress_bar = create_progress_bar(progress, length=12)
                            
                            try:
                                await status_msg.edit_text(
                                    f"📥 **Downloading from Drive ({idx}/{total_files})**\n"
                                    f"📄 `{filename[:45]}...`\n"
                                    f"💾 Size: {format_size(file_size)}\n\n"
                                    f"{progress_bar}\n"
                                    f"⚡ Speed: {format_size(speed)}/s\n"
                                    f"⏱️ ETA: {eta_str}\n\n"
                                    f"✅ {successful} | ❌ {failed}"
                                )
                            except FloodWait as e:
                                logger.warning(f"FloodWait during download: {e.value}s")
                                await asyncio.sleep(e.value)
                            except Exception as e:
                                # Ignore other update errors to avoid breaking download
                                logger.debug(f"Status update error (non-critical): {e}")
                                pass
                
                fh.close()
                
                # Send to Telegram
                await status_msg.edit_text(
                    f"📤 **Sending to Telegram ({idx}/{total_files})**\n"
                    f"📄 `{filename[:50]}...`\n"
                    f"💾 Size: {format_size(file_size)}\n"
                    f"✅ {successful} | ❌ {failed}"
                )
                
                # Determine mime type and send appropriate message
                # IMPORTANT: Check file extensions first for accurate type detection
                
                if download_path.lower().endswith(('.mp3', '.m4a', '.m4b', '.flac', '.wav', '.ogg', '.aac', '.opus', '.wma', '.ape')):
                    # Extract metadata for audio files
                    metadata = extract_audio_metadata(download_path)
                    thumbnail_path = await extract_audio_thumbnail(download_path)
                    
                    # Send with metadata
                    await client.send_audio(
                        status_msg.chat.id,
                        download_path,
                        caption=filename,
                        title=metadata.get('title'),
                        performer=metadata.get('performer'),
                        duration=metadata.get('duration'),
                        thumb=thumbnail_path
                    )
                    
                    # Clean up thumbnail
                    if thumbnail_path and os.path.exists(thumbnail_path):
                        try:
                            os.remove(thumbnail_path)
                        except:
                            pass
                            
                elif download_path.lower().endswith(('.mp4', '.mkv', '.avi', '.mov', '.webm', '.wmv', '.flv', '.3gp')) or 'video' in mime_type:
                    await client.send_video(
                        status_msg.chat.id,
                        download_path,
                        caption=filename
                    )
                elif download_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')) or 'image' in mime_type:
                    await client.send_photo(
                        status_msg.chat.id,
                        download_path,
                        caption=filename
                    )
                else:
                    await client.send_document(
                        status_msg.chat.id,
                        download_path,
                        caption=filename
                    )
                
                # Clean up
                if os.path.exists(download_path):
                    os.remove(download_path)
                
                successful += 1
                logger.info(f"✅ Downloaded and sent: {filename}")
                
            except Exception as e:
                logger.error(f"❌ Error downloading {file_id}: {e}")
                failed += 1
        
        # Final status
        await status_msg.edit_text(
            f"✅ **Download Complete!**\n\n"
            f"✅ Successful: {successful}/{total_files}\n"
            f"❌ Failed: {failed}"
        )
    
    except Exception as e:
        logger.error(f"Download task error: {e}")
        await status_msg.edit_text(f"❌ **Error:** {str(e)}")
    
    finally:
        if task_id in ACTIVE_TASKS:
            del ACTIVE_TASKS[task_id]

async def upload_to_telegram_task(client, status_msg, folders, files, service):
    """Upload selected folders and files from Drive to Telegram"""
    task_id = f"upload_tg_{status_msg.chat.id}_{status_msg.id}"
    ACTIVE_TASKS[task_id] = {
        'cancelled': False,
        'files_list': [],
        'current_file': None,
        'progress': 0,
        'status': 'initializing'
    }
    
    successful = 0
    failed = 0
    start_time = time.time()  # Track start time for elapsed time calculation
    
    try:
        # Collect all files from folders with robust error handling
        all_files = []
        folder_errors = []
        
        # Add direct files
        for file_meta in files:
            all_files.append(file_meta)
        
        # Add files from folders with error recovery
        for folder in folders:
            folder_name = folder['name']
            folder_id = folder['id']
            
            try:
                # Attempt to list files in folder
                _, files_in_folder, _ = list_drive_files(service, folder_id)
                
                if not files_in_folder:
                    logger.warning(f"Folder '{folder_name}' is empty, skipping...")
                    folder_errors.append(f"{folder_name} (empty)")
                    continue
                
                # Add files with folder context
                for file_meta in files_in_folder:
                    file_meta['folder_name'] = folder_name
                    all_files.append(file_meta)
                
                logger.info(f"✅ Added {len(files_in_folder)} files from folder '{folder_name}'")
                
            except HttpError as e:
                logger.error(f"❌ HTTP error accessing folder '{folder_name}': {e}")
                folder_errors.append(f"{folder_name} (access denied)")
            except Exception as e:
                logger.error(f"❌ Error accessing folder '{folder_name}': {e}")
                folder_errors.append(f"{folder_name} (error)")
        
        # Check if we have any files to upload
        if not all_files:
            error_msg = "❌ **No files to upload**\n\n"
            if folder_errors:
                error_msg += "**Failed folders:**\n" + "\n".join(f"• {err}" for err in folder_errors)
            await status_msg.edit_text(error_msg)
            return
        
        # Filter to only audio/video files BEFORE counting
        uploadable_files = []
        skipped_count = 0
        for file_meta in all_files:
            filename = file_meta['name']
            mime_type = file_meta.get('mimeType', '')
            if is_audio_or_video_file(filename, mime_type):
                uploadable_files.append(file_meta)
            else:
                skipped_count += 1
                logger.info(f"⏭️ Skipping non-audio/video file: {filename}")
        
        # Notify about any folder errors and skipped files
        if folder_errors or skipped_count > 0:
            error_note = ""
            if folder_errors:
                error_note = "⚠️ Some folders had issues:\n" + "\n".join(f"• {err}" for err in folder_errors[:5])
            if skipped_count > 0:
                if error_note:
                    error_note += "\n\n"
                error_note += f"⏭️ Skipped {skipped_count} non-audio/video file(s)"
            try:
                await status_msg.edit_text(
                    f"{error_note}\n\n"
                    f"📤 Proceeding with {len(uploadable_files)} audio/video file(s)..."
                )
                await asyncio.sleep(2)
            except:
                pass
        
        # Check if we have uploadable files
        if not uploadable_files:
            await status_msg.edit_text("❌ **No audio/video files to upload**\n\nAll files were filtered out.")
            return
        
        total_files = len(uploadable_files)
        ACTIVE_TASKS[task_id]['files_list'] = [f['id'] for f in uploadable_files]
        
        # Shared state for concurrent workers
        upload_state = {
            'successful': 0,
            'failed': 0,
            'completed': 0,
            'lock': asyncio.Lock(),
            'active_workers': [],
            'stop_updater': False
        }
        
        # Create queue for files
        file_queue = asyncio.Queue()
        for file_meta in uploadable_files:
            await file_queue.put(file_meta)
        
        # Worker function
        async def upload_worker(worker_id):
            while True:
                # Check for cancellation FIRST before pulling from queue
                if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                    break
                
                try:
                    # Get next file from queue with timeout
                    file_meta = await asyncio.wait_for(file_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    # Queue is empty or timeout - check cancellation then exit
                    if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                        break
                    # Queue is empty, worker can exit
                    break
                
                # Double-check for cancellation after getting file
                if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                    file_queue.task_done()
                    break
                
                try:
                    file_id = file_meta['id']
                    filename = file_meta['name']
                    mime_type = file_meta.get('mimeType', '')
                    file_size = int(file_meta.get('size', 0))
                    folder_name = file_meta.get('folder_name', None)
                    
                    # Add to active workers
                    async with upload_state['lock']:
                        upload_state['active_workers'].append({
                            'worker_id': worker_id,
                            'filename': filename,
                            'folder': folder_name,
                            'stage': 'downloading',
                            'progress': 0,
                            'current_bytes': 0,
                            'total_bytes': file_size,
                            'speed': 0,
                            'eta': 'Calculating...'
                        })
                    
                    # Download file from Drive
                    request = service.files().get_media(fileId=file_id)
                    download_path = f"downloads/{filename}"
                    
                    os.makedirs("downloads", exist_ok=True)
                    
                    fh = io.FileIO(download_path, 'wb')
                    downloader = MediaIoBaseDownload(fh, request)
                    
                    done = False
                    last_update = 0
                    drive_start_time = time.time()
                    last_drive_bytes = 0
                    last_drive_speed_update = drive_start_time
                    
                    while not done:
                        if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                            fh.close()
                            if os.path.exists(download_path):
                                os.remove(download_path)
                            file_queue.task_done()
                            return
                        
                        status, done = downloader.next_chunk()
                        
                        # Update progress with speed and ETA
                        if status:
                            progress = int(status.progress() * 100)
                            current_bytes = int(status.progress() * file_size)
                            current_time = time.time()
                            
                            # Only update every 3 seconds to avoid FloodWait
                            if current_time - last_update >= 3 or done:
                                # Calculate speed
                                time_diff = current_time - last_drive_speed_update
                                bytes_diff = current_bytes - last_drive_bytes
                                
                                if time_diff > 0:
                                    drive_speed = bytes_diff / time_diff
                                else:
                                    drive_speed = 0
                                
                                # Calculate ETA
                                if drive_speed > 0 and current_bytes < file_size:
                                    remaining_bytes = file_size - current_bytes
                                    eta_seconds = remaining_bytes / drive_speed
                                    eta_str = format_time(eta_seconds)
                                else:
                                    eta_str = "Calculating..." if current_bytes < file_size else "Done"
                                
                                # Update tracking
                                last_update = current_time
                                last_drive_speed_update = current_time
                                last_drive_bytes = current_bytes
                                
                                # Update worker state
                                async with upload_state['lock']:
                                    for worker in upload_state['active_workers']:
                                        if worker['worker_id'] == worker_id:
                                            worker['progress'] = progress
                                            worker['current_bytes'] = current_bytes
                                            worker['total_bytes'] = file_size
                                            worker['speed'] = int(drive_speed)
                                            worker['eta'] = eta_str
                                            break
                    
                    fh.close()
                    
                    # Update worker stage to uploading and reset progress
                    async with upload_state['lock']:
                        for worker in upload_state['active_workers']:
                            if worker['worker_id'] == worker_id:
                                worker['stage'] = 'uploading'
                                worker['progress'] = 0
                                worker['current_bytes'] = 0
                                worker['speed'] = 0
                                worker['eta'] = 'Starting...'
                                break
                    
                    # Send to Telegram with progress tracking
                    mime_type = file_meta.get('mimeType', '')
                    is_audio = download_path.lower().endswith(('.mp3', '.m4a', '.m4b', '.flac', '.wav', '.ogg', '.aac', '.opus', '.wma', '.ape'))
                    is_video = download_path.lower().endswith(('.mp4', '.mkv', '.avi', '.mov', '.webm', '.wmv', '.flv', '.3gp')) or 'video' in mime_type
                    
                    # Set caption
                    if is_video:
                        caption = None
                    elif is_audio and folder_name:
                        caption = f"📖 {folder_name}"
                    elif is_audio:
                        caption = filename
                    else:
                        caption = filename
                    
                    # Progress callback for Telegram upload
                    upload_start_time = time.time()
                    last_progress_update = 0
                    
                    async def progress_callback(current, total):
                        nonlocal last_progress_update
                        current_time = time.time()
                        
                        # Update every 2 seconds to avoid FloodWait
                        if current_time - last_progress_update >= 2:
                            last_progress_update = current_time
                            
                            # Calculate speed
                            elapsed = current_time - upload_start_time
                            speed = current / elapsed if elapsed > 0 else 0
                            
                            # Calculate ETA
                            remaining = total - current
                            eta = remaining / speed if speed > 0 else 0
                            eta_str = format_time(int(eta))
                            
                            # Update worker state
                            progress_pct = int((current / total) * 100) if total > 0 else 0
                            
                            async with upload_state['lock']:
                                for worker in upload_state['active_workers']:
                                    if worker['worker_id'] == worker_id:
                                        worker['progress'] = progress_pct
                                        worker['current_bytes'] = current
                                        worker['total_bytes'] = total
                                        worker['speed'] = int(speed)
                                        worker['eta'] = eta_str
                                        break
                    
                    # Send based on file type
                    if download_path.lower().endswith(('.mp3', '.m4a', '.m4b', '.flac', '.wav', '.ogg', '.aac', '.opus', '.wma', '.ape')):
                        # Extract metadata for audio files
                        metadata = extract_audio_metadata(download_path)
                        thumbnail_path = await extract_audio_thumbnail(download_path)
                        
                        # Send with metadata and progress
                        await client.send_audio(
                            status_msg.chat.id,
                            download_path,
                            caption=caption,
                            title=metadata.get('title'),
                            performer=metadata.get('performer'),
                            duration=metadata.get('duration'),
                            thumb=thumbnail_path,
                            progress=progress_callback
                        )
                        
                        # Clean up thumbnail
                        if thumbnail_path and os.path.exists(thumbnail_path):
                            try:
                                os.remove(thumbnail_path)
                            except:
                                pass
                                
                    elif download_path.lower().endswith(('.mp4', '.mkv', '.avi', '.mov', '.webm', '.wmv', '.flv', '.3gp')) or 'video' in mime_type:
                        await client.send_video(
                            status_msg.chat.id,
                            download_path,
                            caption=caption,
                            progress=progress_callback
                        )
                    elif download_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')) or 'image' in mime_type:
                        await client.send_photo(
                            status_msg.chat.id,
                            download_path,
                            caption=caption,
                            progress=progress_callback
                        )
                    else:
                        await client.send_document(
                            status_msg.chat.id,
                            download_path,
                            caption=caption,
                            progress=progress_callback
                        )
                    
                    # Clean up
                    if os.path.exists(download_path):
                        os.remove(download_path)
                    
                    async with upload_state['lock']:
                        upload_state['successful'] += 1
                        upload_state['completed'] += 1
                    
                    logger.info(f"✅ Worker {worker_id} uploaded: {filename}")
                    
                except Exception as e:
                    logger.error(f"❌ Worker {worker_id} error on {filename}: {e}")
                    async with upload_state['lock']:
                        upload_state['failed'] += 1
                        upload_state['completed'] += 1
                
                finally:
                    # Remove from active workers
                    async with upload_state['lock']:
                        upload_state['active_workers'] = [
                            w for w in upload_state['active_workers'] 
                            if w['worker_id'] != worker_id
                        ]
                    file_queue.task_done()
        
        # Status updater task - updates message with all active workers
        async def status_updater():
            while not upload_state['stop_updater']:
                try:
                    async with upload_state['lock']:
                        completed = upload_state['completed']
                        successful = upload_state['successful']
                        failed = upload_state['failed']
                        workers = list(upload_state['active_workers'])
                    
                    # Build status message with all workers
                    status_lines = [
                        f"📤 **Uploading to Telegram**",
                        f"━━━━━━━━━━━━━━━",
                        f"📊 Overall: {completed}/{total_files}",
                        f"✅ Success: {successful} | ❌ Failed: {failed}",
                        ""
                    ]
                    
                    if workers:
                        status_lines.append("🔄 **Active Workers:**")
                        status_lines.append("")
                        
                        for worker in workers:
                            worker_lines = []
                            stage_icon = "📥" if worker['stage'] == 'downloading' else "📤"
                            
                            # Worker header
                            worker_lines.append(f"{stage_icon} **Worker {worker['worker_id']}:**")
                            
                            # Folder if present
                            if worker.get('folder'):
                                worker_lines.append(f"📁 {worker['folder'][:30]}")
                            
                            # Filename
                            worker_lines.append(f"📄 `{worker['filename'][:35]}...`")
                            
                            # Progress info if available
                            if worker.get('progress') is not None and worker['progress'] > 0:
                                progress_bar = create_progress_bar(worker['progress'], length=12)
                                worker_lines.append(f"{progress_bar}")
                                
                                if worker.get('current_bytes') and worker.get('total_bytes'):
                                    worker_lines.append(
                                        f"💾 {format_size(worker['current_bytes'])} / {format_size(worker['total_bytes'])}"
                                    )
                                
                                if worker.get('speed'):
                                    worker_lines.append(f"⚡ {format_size(worker['speed'])}/s")
                                
                                if worker.get('eta'):
                                    worker_lines.append(f"⏱️ {worker['eta']}")
                            
                            # Add worker info to status
                            status_lines.extend(worker_lines)
                            status_lines.append("")  # Empty line between workers
                    
                    # Update message
                    try:
                        await status_msg.edit_text("\n".join(status_lines))
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
                    except:
                        pass
                    
                    # Update every 1 second
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.debug(f"Status updater error: {e}")
                    await asyncio.sleep(1)
        
        # Start 2 concurrent workers
        worker_tasks = [
            asyncio.create_task(upload_worker(1)),
            asyncio.create_task(upload_worker(2))
        ]
        
        # Start status updater
        updater_task = asyncio.create_task(status_updater())
        
        # Wait for workers to complete
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        
        # Wait for queue to be fully processed
        await file_queue.join()
        
        # Stop status updater
        upload_state['stop_updater'] = True
        try:
            await asyncio.wait_for(updater_task, timeout=2.0)
        except:
            pass
        
        # Final status
        elapsed_time = time.time() - start_time
        successful = upload_state['successful']
        failed = upload_state['failed']
        
        # Check if task was cancelled
        if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
            status_text = (
                f"🛑 **Upload Cancelled**\n"
                f"━━━━━━━━━━━━━━━\n\n"
                f"✅ Sent: {successful}/{total_files}\n"
                f"❌ Failed: {failed}\n"
                f"⏱️ Time: {format_time(elapsed_time)}"
            )
        else:
            status_text = (
                f"✅ **Upload to Telegram Complete!**\n"
                f"━━━━━━━━━━━━━━━\n\n"
                f"✅ Successful: {successful}/{total_files}\n"
            )
            
            if failed > 0:
                status_text += f"❌ Failed: {failed}\n"
            
            status_text += f"⏱️ Time: {format_time(elapsed_time)}\n"
            
            if successful > 0 and elapsed_time > 0:
                avg_time_per_file = elapsed_time / successful
                status_text += f"⚡ Avg: {format_time(avg_time_per_file)}/file"
        
        await status_msg.edit_text(status_text)
    
    except Exception as e:
        logger.error(f"Upload to Telegram task error: {e}")
        await status_msg.edit_text(f"❌ **Error:** {str(e)}")
    
    finally:
        if task_id in ACTIVE_TASKS:
            del ACTIVE_TASKS[task_id]

def upload_to_drive_with_progress(service, file_path, file_metadata, message_data, filename):
    """
    Upload file to Google Drive with progress tracking
    message_data is a dict with {'chat_id': int, 'message_id': int, 'bot': Client, 'task_id': str}
    """
    try:
        media = MediaFileUpload(
            file_path,
            resumable=True,
            chunksize=5*1024*1024  # 5 MB chunks for better progress updates
        )
        
        request = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, size, webViewLink'
        )
        
        response = None
        last_progress = 0
        last_update = time.time()
        file_size = os.path.getsize(file_path)
        start_time = time.time()
        last_uploaded = 0
        
        while response is None:
            # Check for cancellation
            task_id = message_data.get('task_id')
            if task_id and ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                message_data['cancelled'] = True
                return None
            
            status, response = request.next_chunk()
            
            if status:
                progress = int(status.progress() * 100)
                current_bytes = int(status.progress() * file_size)
                now = time.time()
                
                # Update every 2 seconds or on significant progress change
                if (now - last_update) > 2 or progress - last_progress >= 5:
                    # Calculate speed
                    time_diff = now - last_update if (now - last_update) > 0 else 0.1
                    bytes_diff = current_bytes - last_uploaded
                    speed = bytes_diff / time_diff if time_diff > 0 else 0
                    
                    # Progress bar
                    filled = int(progress // 10)
                    bar = '■' * filled + '□' * (10 - filled)
                    
                    # ETA
                    if speed > 0 and current_bytes < file_size:
                        remaining = file_size - current_bytes
                        eta_seconds = remaining / speed
                        eta = str(datetime.timedelta(seconds=int(eta_seconds)))
                    else:
                        eta = "Calculating..." if current_bytes < file_size else "Done"
                    
                    # Update message (will be called from executor, but we store the data)
                    message_data['last_progress'] = {
                        'bar': bar,
                        'progress': progress,
                        'speed': speed,
                        'current': current_bytes,
                        'total': file_size,
                        'eta': eta,
                        'filename': filename
                    }
                    
                    last_progress = progress
                    last_update = now
                    last_uploaded = current_bytes
        
        # Mark as complete
        message_data['complete'] = True
        return response
    
    except Exception as e:
        logger.error(f"Upload error: {e}")
        message_data['error'] = str(e)
        raise

# ==================== PROGRESS CALLBACK ====================
async def progress_callback(current, total, message, start_time, filename, task_id):
    """Show upload/download progress with accurate speed and cancellation support"""
    # Check for cancellation
    if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
        raise Exception("Download cancelled by user")
    
    now = time.time()
    
    # Initialize tracking variables
    if not hasattr(message, 'last_update'):
        message.last_update = 0
        message.last_current = 0
        message.last_time = start_time
    
    # Update every 2 seconds or on completion
    if (now - message.last_update) > 2 or current == total:
        # Calculate incremental speed (more accurate)
        time_diff = now - message.last_time
        bytes_diff = current - message.last_current
        
        if time_diff > 0:
            speed = bytes_diff / time_diff  # Bytes per second
        else:
            speed = 0
        
        percentage = (current / total) * 100
        
        # Progress bar
        filled = int(percentage // 10)
        bar = '■' * filled + '□' * (10 - filled)
        
        # ETA calculation
        if speed > 0 and current < total:
            remaining_bytes = total - current
            eta_seconds = remaining_bytes / speed
            eta = str(datetime.timedelta(seconds=int(eta_seconds)))
        else:
            eta = "Calculating..." if current < total else "Done"
        
        try:
            cancel_button = InlineKeyboardMarkup([
                [InlineKeyboardButton("🛑 Cancel Upload", callback_data=f"cancel_{task_id}")]
            ])
            
            await message.edit_text(
                f"📥 **Downloading from Telegram**\n"
                f"📄 `{filename[:40]}...`\n\n"
                f"[{bar}] {percentage:.1f}%\n"
                f"⚡ Speed: {speed/1024/1024:.2f} MB/s\n"
                f"💾 {current/1024/1024:.1f} MB / {total/1024/1024:.1f} MB\n"
                f"⏱️ ETA: {eta}",
                reply_markup=cancel_button
            )
        except Exception:
            pass
        
        # Update tracking variables
        message.last_update = now
        message.last_current = current
        message.last_time = now

async def upload_task(client: Client, status_msg: Message, file_list: list, series_name: str = None, flat_upload: bool = False, queue_id: str = None, parent_folder_id: str = None):
    """
    Main upload task - handles batch uploading with proper organization and cancellation
    
    Args:
        client: Pyrogram client
        status_msg: Status message to update
        file_list: List of dicts with 'msg_id' and 'name'
        series_name: Optional series name for organization
        flat_upload: If True, upload directly to parent folder without subfolders
        queue_id: Optional queue ID for tracking
        parent_folder_id: NEW - Optional parent folder ID (defaults to DRIVE_FOLDER_ID)
    """
    global TOTAL_FILES, TOTAL_BYTES
    
    # NEW: Use provided parent_folder_id or default to DRIVE_FOLDER_ID
    upload_parent = parent_folder_id if parent_folder_id else DRIVE_FOLDER_ID
    
    task_id = f"{status_msg.chat.id}_{status_msg.id}"
    ACTIVE_TASKS[task_id] = {
        'cancelled': False, 
        'files_list': file_list,
        'current_file': None,
        'progress': 0,
        'status': 'initializing'
    }
    
    # Update queue status
    if queue_id and queue_id in UPLOAD_QUEUE:
        UPLOAD_QUEUE[queue_id]['status'] = 'uploading'
    
    service = get_drive_service()
    if not service:
        await status_msg.edit_text("❌ **Failed to connect to Google Drive!**\n\nPlease check TOKEN_JSON configuration.")
        if task_id in ACTIVE_TASKS:
            del ACTIVE_TASKS[task_id]
        if queue_id and queue_id in UPLOAD_QUEUE:
            UPLOAD_QUEUE[queue_id]['status'] = 'failed'
        return
    
    try:
        await status_msg.edit_text(f"🔄 **Preparing folders...**\n📦 Files to upload: {len(file_list)}")
        
        # MODIFIED: Determine parent folder - use upload_parent instead of DRIVE_FOLDER_ID
        if series_name and not flat_upload:
            # Create series folder in the selected parent
            parent_folder = get_or_create_folder(service, series_name, upload_parent)
            if not parent_folder:
                await status_msg.edit_text(f"❌ **Failed to create series folder:** {series_name}")
                if task_id in ACTIVE_TASKS:
                    del ACTIVE_TASKS[task_id]
                if queue_id and queue_id in UPLOAD_QUEUE:
                    UPLOAD_QUEUE[queue_id]['status'] = 'failed'
                return
        else:
            # Use the selected parent folder directly
            parent_folder = upload_parent
        
        # Create downloads directory if not exists
        os.makedirs("downloads", exist_ok=True)
        
        # Shared state for concurrent workers
        upload_state = {
            'successful_uploads': 0,
            'failed_uploads': [],
            'total_size_uploaded': 0,
            'completed': 0,
            'lock': asyncio.Lock(),
            'active_workers': [],
            'stop_updater': False
        }
        
        # Create queue for files
        file_queue = asyncio.Queue()
        for file_info in file_list:
            await file_queue.put(file_info)
        
        # Worker function
        async def upload_worker(worker_id):
            while True:
                try:
                    # Get next file from queue
                    file_info = await asyncio.wait_for(file_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    # Queue is empty, worker can exit
                    break
                
                # Check for cancellation
                if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                    file_queue.task_done()
                    break
                
                filename = file_info['name']
                msg_id = file_info['msg_id']
                download_path = None
                
                # Clean the filename
                clean_name = clean_filename(filename)
                
                # Add to active workers
                async with upload_state['lock']:
                    upload_state['active_workers'].append({
                        'worker_id': worker_id,
                        'filename': filename,
                        'stage': 'downloading',
                        'progress': 0,
                        'current_bytes': 0,
                        'total_bytes': 0,
                        'speed': 0,
                        'eta': 'Starting...'
                    })
                
                try:
                    # Auto-retry logic
                    retry_count = 0
                    upload_success = False
                    
                    while retry_count < MAX_RETRIES and not upload_success:
                        try:
                            # Check for cancellation
                            if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                                file_queue.task_done()
                                return
                            
                            # Download file from Telegram
                            download_path = f"downloads/{filename}"
                            download_start = time.time()
                            last_download_update = 0
                            
                            # Progress callback for Telegram download
                            async def download_progress_callback(current, total):
                                nonlocal last_download_update
                                current_time = time.time()
                                
                                # Update every 1.5 seconds
                                if current_time - last_download_update >= 1.5:
                                    last_download_update = current_time
                                    
                                    progress_pct = int((current / total) * 100) if total > 0 else 0
                                    elapsed = current_time - download_start
                                    speed = current / elapsed if elapsed > 0 else 0
                                    remaining = total - current
                                    eta = remaining / speed if speed > 0 else 0
                                    eta_str = format_time(int(eta))
                                    
                                    # Update worker state
                                    async with upload_state['lock']:
                                        for worker in upload_state['active_workers']:
                                            if worker['worker_id'] == worker_id:
                                                worker['progress'] = progress_pct
                                                worker['current_bytes'] = current
                                                worker['total_bytes'] = total
                                                worker['speed'] = int(speed)
                                                worker['eta'] = eta_str
                                                break
                            
                            try:
                                message = await client.get_messages(status_msg.chat.id, msg_id)
                                await client.download_media(
                                    message,
                                    file_name=download_path,
                                    progress=download_progress_callback
                                )
                            except FloodWait as e:
                                logger.warning(f"Worker {worker_id}: FloodWait {e.value}s")
                                await asyncio.sleep(e.value)
                                message = await client.get_messages(status_msg.chat.id, msg_id)
                                await client.download_media(
                                    message,
                                    file_name=download_path,
                                    progress=download_progress_callback
                                )
                            
                            # Check for cancellation after download
                            if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                                if download_path and os.path.exists(download_path):
                                    os.remove(download_path)
                                file_queue.task_done()
                                return
                            
                            if not os.path.exists(download_path):
                                raise Exception("Download failed - file not found")
                            
                            file_size = os.path.getsize(download_path)
                            
                            # Handle folder logic
                            if flat_upload:
                                upload_folder = parent_folder
                            else:
                                folder_name = os.path.splitext(clean_name)[0]
                                file_folder = get_or_create_folder(service, folder_name, parent_folder)
                                
                                if not file_folder:
                                    raise Exception("Failed to create file folder")
                                
                                upload_folder = file_folder
                            
                            # Upload to Drive
                            file_metadata = {
                                'name': clean_name,
                                'parents': [upload_folder]
                            }
                            
                            # Shared data for progress tracking
                            progress_data = {
                                'complete': False,
                                'error': None,
                                'last_progress': None,
                                'task_id': task_id,
                                'cancelled': False
                            }
                            
                            # Start upload in executor
                            loop = asyncio.get_running_loop()
                            upload_future = loop.run_in_executor(
                                None,
                                upload_to_drive_with_progress,
                                service,
                                download_path,
                                file_metadata,
                                progress_data,
                                filename
                            )
                            
                            # Update worker stage to uploading
                            async with upload_state['lock']:
                                for worker in upload_state['active_workers']:
                                    if worker['worker_id'] == worker_id:
                                        worker['stage'] = 'uploading'
                                        worker['progress'] = 0
                                        worker['current_bytes'] = 0
                                        worker['total_bytes'] = file_size
                                        worker['speed'] = 0
                                        worker['eta'] = 'Starting...'
                                        break
                            
                            # Monitor progress and update worker state
                            last_progress_update = time.time()
                            while not progress_data.get('complete') and not progress_data.get('error') and not progress_data.get('cancelled'):
                                if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                                    progress_data['cancelled'] = True
                                    break
                                
                                # Update worker state from progress_data
                                current_time = time.time()
                                if current_time - last_progress_update >= 1:
                                    last_progress_update = current_time
                                    last_prog = progress_data.get('last_progress')
                                    
                                    if last_prog:
                                        async with upload_state['lock']:
                                            for worker in upload_state['active_workers']:
                                                if worker['worker_id'] == worker_id:
                                                    worker['progress'] = last_prog.get('progress', 0)
                                                    worker['current_bytes'] = last_prog.get('current', 0)
                                                    worker['total_bytes'] = last_prog.get('total', file_size)
                                                    worker['speed'] = int(last_prog.get('speed', 0))
                                                    worker['eta'] = last_prog.get('eta', 'Calculating...')
                                                    break
                                
                                await asyncio.sleep(0.5)
                            
                            # Check if cancelled
                            if progress_data.get('cancelled') or ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                                if download_path and os.path.exists(download_path):
                                    os.remove(download_path)
                                file_queue.task_done()
                                return
                            
                            # Wait for upload to complete
                            upload_result = await upload_future
                            
                            if progress_data.get('error'):
                                raise Exception(progress_data['error'])
                            
                            if upload_result is None:
                                raise Exception("Upload was cancelled")
                            
                            # Update stats
                            uploaded_size = int(upload_result.get('size', file_size))
                            
                            async with upload_state['lock']:
                                upload_state['successful_uploads'] += 1
                                upload_state['total_size_uploaded'] += uploaded_size
                                upload_state['completed'] += 1
                                
                                # Update global stats
                                global TOTAL_FILES, TOTAL_BYTES
                                TOTAL_FILES += 1
                                TOTAL_BYTES += uploaded_size
                            
                            # Clean up downloaded file
                            if os.path.exists(download_path):
                                os.remove(download_path)
                            
                            upload_success = True
                            logger.info(f"✅ Worker {worker_id}: {filename} ({uploaded_size/1024/1024:.2f} MB)")
                        
                        except Exception as e:
                            retry_count += 1
                            logger.error(f"❌ Worker {worker_id}: {filename} (attempt {retry_count}/{MAX_RETRIES}): {e}")
                            
                            if retry_count < MAX_RETRIES:
                                wait_time = RETRY_DELAY * (2 ** (retry_count - 1))
                                logger.info(f"Worker {worker_id} retrying in {wait_time}s...")
                                await asyncio.sleep(wait_time)
                            else:
                                async with upload_state['lock']:
                                    upload_state['failed_uploads'].append(f"{filename}: {str(e)[:50]}")
                                    upload_state['completed'] += 1
                                record_failed_upload(task_id, filename, e)
                            
                            # Clean up on error
                            if download_path and os.path.exists(download_path):
                                try:
                                    os.remove(download_path)
                                except:
                                    pass
                
                except Exception as e:
                    # Catch any unexpected errors in the worker
                    logger.error(f"Worker {worker_id} unexpected error: {e}")
                    async with upload_state['lock']:
                        upload_state['failed_uploads'].append(f"{filename}: {str(e)[:50]}")
                        upload_state['completed'] += 1
                
                finally:
                    # Remove from active workers
                    async with upload_state['lock']:
                        upload_state['active_workers'] = [
                            w for w in upload_state['active_workers']
                            if w['worker_id'] != worker_id
                        ]
                    file_queue.task_done()
        
        # Status updater task - updates message with all active workers
        async def status_updater():
            while not upload_state['stop_updater']:
                try:
                    async with upload_state['lock']:
                        completed = upload_state['completed']
                        successful = upload_state['successful_uploads']
                        failed = len(upload_state['failed_uploads'])
                        workers = list(upload_state['active_workers'])
                    
                    # Build status message with all workers
                    status_lines = [
                        f"📤 **Uploading to Google Drive**",
                        f"━━━━━━━━━━━━━━━",
                        f"📊 Overall: {completed}/{len(file_list)}",
                        f"✅ Success: {successful} | ❌ Failed: {failed}",
                        ""
                    ]
                    
                    if workers:
                        status_lines.append("🔄 **Active Workers:**")
                        status_lines.append("")
                        
                        for worker in workers:
                            worker_lines = []
                            stage_icon = "📥" if worker['stage'] == 'downloading' else "☁️"
                            
                            # Worker header
                            worker_lines.append(f"{stage_icon} **Worker {worker['worker_id']}:**")
                            
                            # Filename
                            worker_lines.append(f"📄 `{worker['filename'][:35]}...`")
                            
                            # Progress info if available
                            if worker.get('progress') is not None and worker['progress'] > 0:
                                progress_bar = create_progress_bar(worker['progress'], length=12)
                                worker_lines.append(f"{progress_bar}")
                                
                                if worker.get('current_bytes') and worker.get('total_bytes'):
                                    worker_lines.append(
                                        f"💾 {format_size(worker['current_bytes'])} / {format_size(worker['total_bytes'])}"
                                    )
                                
                                if worker.get('speed'):
                                    worker_lines.append(f"⚡ {format_size(worker['speed'])}/s")
                                
                                if worker.get('eta'):
                                    worker_lines.append(f"⏱️ {worker['eta']}")
                            
                            # Add worker info to status
                            status_lines.extend(worker_lines)
                            status_lines.append("")  # Empty line between workers
                    
                    # Update message
                    try:
                        await status_msg.edit_text("\n".join(status_lines))
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
                    except:
                        pass
                    
                    # Update every 1 second
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.debug(f"Status updater error: {e}")
                    await asyncio.sleep(1)
        
        # Start 2 concurrent workers
        worker_tasks = [
            asyncio.create_task(upload_worker(1)),
            asyncio.create_task(upload_worker(2))
        ]
        
        # Start status updater
        updater_task = asyncio.create_task(status_updater())
        
        # Wait for workers to complete
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        
        # Wait for queue to be fully processed
        await file_queue.join()
        
        # Stop status updater
        upload_state['stop_updater'] = True
        try:
            await asyncio.wait_for(updater_task, timeout=2.0)
        except:
            pass
        
        # Get final counts
        successful_uploads = upload_state['successful_uploads']
        failed_uploads = upload_state['failed_uploads']
        total_size_uploaded = upload_state['total_size_uploaded']
        
        # Update task progress
        ACTIVE_TASKS[task_id]['progress'] = 100
        ACTIVE_TASKS[task_id]['status'] = 'completed'
        
        # Save stats
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, sync_stats, service, True)
        except Exception as e:
            logger.error(f"Failed to save stats: {e}")
        
        # Final status
        elapsed_time = time.time() - start_time
        location_text = "Root (No Folders)" if flat_upload else (f"**{series_name}**" if series_name else "Root -> Standalone")
        
        status_text = (
            f"✅ **Upload Complete!**\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"📁 Location: {location_text}\n"
            f"✅ Successful: {successful_uploads}/{len(file_list)}\n"
            f"📊 Total Size: {format_size(total_size_uploaded)}\n"
            f"⏱️ Time: {format_time(elapsed_time)}\n"
        )
        
        if elapsed_time > 0:
            status_text += f"⚡ Avg Speed: {format_size(total_size_uploaded/elapsed_time)}/s\n"
        
        if failed_uploads:
            status_text += (
                f"\n❌ **Failed: {len(failed_uploads)}**\n"
                f"Use `/retry` to retry failed uploads\n\n"
                f"**Errors:**\n"
            )
            for error in failed_uploads[:5]:  # Show first 5 errors
                status_text += f"• {error}\n"
        
        status_text += (
            f"\n━━━━━━━━━━━━━━━\n"
            f"📈 **Total Stats**\n"
            f"📁 Files: {TOTAL_FILES:,} | 💾 Size: {TOTAL_BYTES/1024/1024/1024:.2f} GB"
        )
        
        await status_msg.edit_text(status_text)
        
        # Update queue status
        if queue_id and queue_id in UPLOAD_QUEUE:
            UPLOAD_QUEUE[queue_id]['status'] = 'completed' if not failed_uploads else 'completed_with_errors'
    
    except Exception as e:
        logger.error(f"Upload task error: {e}")
        await status_msg.edit_text(f"❌ **Error:** {str(e)}")
        if queue_id and queue_id in UPLOAD_QUEUE:
            UPLOAD_QUEUE[queue_id]['status'] = 'failed'
    
    finally:
        # Clean up task
        if task_id in ACTIVE_TASKS:
            del ACTIVE_TASKS[task_id]
# ==================== BOT HANDLERS ====================
@app.on_message(filters.command("start") & ~filters.user(OWNER_ID))
async def start_unauthorized(client, message):
    """Snarky response for unauthorized users"""
    snarky_messages = [
        "🚫 Nice try, but this bot is **RxxFii's personal butler**.\n\nYou? You're not on the list. 💅",
        "🙄 Oh look, another stranger trying to use my services.\n\n**Access Denied.** Get your own bot.",
        "🤖 *Beep boop* Error 403: **You're not RxxFii**\n\nThis bot doesn't work for peasants. 👑",
        "😂 Did you really think this would work?\n\nThis is a **VIP-only bot**. Spoiler: You're not VIP.",
        "🔒 **UNAUTHORIZED ACCESS DETECTED**\n\nThis bot serves one master: RxxFii.\n\nYou ain't it, chief.",
        "🎭 Roses are red, violets are blue,\nThis bot's for RxxFii, not for you. 💐",
        "⛔ **Access Denied**\n\nWhat part of 'private bot' do you not understand?\n\n*shoo* 👋"
    ]
    
    import random
    await message.reply_text(random.choice(snarky_messages))

@app.on_message(filters.command("start") & filters.user(OWNER_ID))
async def start_command(client, message):
    """Welcome message"""
    uptime = format_time(time.time() - START_TIME)
    
    await message.reply_text(
        "🤖 **RxUploader Bot**\n"
        "━━━━━━━━━━━━━━━\n\n"
        "**📤 Upload to Drive**\n"
        "Send files → Auto-upload to Google Drive\n"
        "• Series auto-detection ✅\n"
        "• Queue management ✅\n"
        "• Real-time progress ✅\n\n"
        "**📥 Download from Drive**\n"
        "Send Drive link → Get files instantly\n"
        "• Single files ✅\n"
        "• Entire folders ✅\n\n"
        "**⚙️ Commands**\n"
        "/stats - View statistics\n"
        "/queue - Check upload queue\n"
        "/browse - Browse your Drive\n"
        "/search <query> - Search files\n"
        "/cancel - Cancel uploads\n"
        "/retry - Retry failed uploads\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏱️ Uptime: {uptime}\n"
        f"📊 Uploaded: {TOTAL_FILES:,} files\n"
        f"💾 Total: {TOTAL_BYTES/1024/1024/1024:.2f} GB"
    )

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_command(client, message):
    """Show upload statistics"""
    uptime = str(datetime.timedelta(seconds=int(time.time() - START_TIME)))
    
    stats_text = (
        f"📊 **Bot Statistics**\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"⏱️ Uptime: `{uptime}`\n"
        f"📁 Total Files: `{TOTAL_FILES:,}`\n"
        f"💾 Total Data: `{TOTAL_BYTES/1024/1024/1024:.2f} GB`\n"
    )
    
    if TOTAL_FILES > 0:
        stats_text += f"📈 Avg File: `{(TOTAL_BYTES/TOTAL_FILES/1024/1024):.2f} MB`"
    
    # Show active tasks
    if ACTIVE_TASKS:
        stats_text += f"\n\n━━━━━━━━━━━━━━━\n🔄 **Active Tasks:** {len(ACTIVE_TASKS)}"
    
    await message.reply_text(stats_text)

@app.on_message(filters.command("cancel") & filters.user(OWNER_ID))
async def cancel_command(client, message):
    """Cancel active upload/download tasks"""
    if not ACTIVE_TASKS:
        await message.reply_text("ℹ️ No active tasks to cancel")
        return
    
    # Cancel all active tasks
    cancelled_count = 0
    for task_id in list(ACTIVE_TASKS.keys()):
        ACTIVE_TASKS[task_id]['cancelled'] = True
        cancelled_count += 1
        logger.info(f"Cancelling task: {task_id}")
    
    await message.reply_text(
        f"🛑 **Cancellation Requested**\n\n"
        f"Tasks being cancelled: {cancelled_count}\n"
        f"Please wait for tasks to stop..."
    )

@app.on_message(filters.command("queue") & filters.user(OWNER_ID))
async def queue_command(client, message):
    """Show current upload queue"""
    queue_text = get_queue_status()
    
    if ACTIVE_TASKS:
        queue_text += f"\n\n🔄 **Active Tasks:** {len(ACTIVE_TASKS)}\n"
        for task_id, task_data in ACTIVE_TASKS.items():
            current_file = task_data.get('current_file', 'N/A')
            progress = task_data.get('progress', 0)
            status = task_data.get('status', 'unknown')
            queue_text += f"\n📌 `{task_id[:20]}...`\n"
            queue_text += f"  File: {current_file[:30]}...\n"
            queue_text += f"  Progress: {progress}% | Status: {status}\n"
    
    await message.reply_text(queue_text)

@app.on_message(filters.command("status") & filters.user(OWNER_ID))
async def status_command(client, message):
    """Show detailed status of all operations"""
    status_text = (
        "📊 **Bot Status**\n"
        "━━━━━━━━━━━━━━━\n\n"
    )
    
    # Active tasks
    if ACTIVE_TASKS:
        status_text += f"🔄 **Active Uploads:** {len(ACTIVE_TASKS)}\n\n"
        for task_id, task_data in ACTIVE_TASKS.items():
            current_file = task_data.get('current_file', 'Initializing...')
            progress = task_data.get('progress', 0)
            status = task_data.get('status', 'unknown')
            files_count = len(task_data.get('files_list', []))
            
            status_text += f"📌 `{task_id[:25]}...`\n"
            status_text += f"  📄 {current_file[:35]}...\n"
            status_text += f"  📊 {progress}% • {status}\n"
            status_text += f"  📁 {files_count} files total\n\n"
    else:
        status_text += "✅ No active uploads\n\n"
    
    # Queue status
    status_text += "━━━━━━━━━━━━━━━\n"
    if UPLOAD_QUEUE:
        status_text += f"📋 **Queued:** {len(UPLOAD_QUEUE)}\n"
        for queue_id, data in list(UPLOAD_QUEUE.items())[:5]:  # Show first 5
            status_text += f"  • {queue_id}: {len(data['files'])} files ({data['status']})\n"
    else:
        status_text += "📭 Queue is empty\n"
    
    # Failed uploads
    if FAILED_UPLOADS:
        status_text += (
            f"\n━━━━━━━━━━━━━━━\n"
            f"❌ **Failed:** {len(FAILED_UPLOADS)} tasks\n"
        )
        total_failed = sum(len(data['files']) for data in FAILED_UPLOADS.values())
        status_text += f"  📊 {total_failed} files failed\n"
        status_text += f"  💡 Use `/retry` to retry\n"
    
    # System stats
    uptime = str(datetime.timedelta(seconds=int(time.time() - START_TIME)))
    status_text += (
        f"\n━━━━━━━━━━━━━━━\n"
        f"⏱️ **Uptime:** {uptime}\n"
        f"📈 **Total Uploaded:** {TOTAL_FILES:,} files ({TOTAL_BYTES/1024/1024/1024:.2f} GB)\n"
        f"💾 **Browser Sessions:** {len(BROWSER_SESSIONS)} active"
    )
    
    # NEW: Add cancel buttons for each active task
    if ACTIVE_TASKS:
        keyboard = []
        for task_id in list(ACTIVE_TASKS.keys())[:5]:  # Max 5 buttons
            keyboard.append([InlineKeyboardButton(
                f"🛑 Cancel {task_id[:15]}...", 
                callback_data=f"cancel_{task_id}"
            )])
        
        await message.reply_text(status_text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await message.reply_text(status_text)

@app.on_message(filters.command("retry") & filters.user(OWNER_ID))
async def retry_command(client, message):
    """Retry failed uploads"""
    if not FAILED_UPLOADS:
        await message.reply_text("✅ No failed uploads to retry")
        return
    
    # Show failed uploads
    retry_text = "🔄 **Failed Uploads Available for Retry**\n\n"
    
    for task_id, data in list(FAILED_UPLOADS.items())[:5]:  # Show first 5 tasks
        retry_text += f"📌 Task: `{task_id[:25]}...`\n"
        retry_text += f"  Failed files: {len(data['files'])}\n"
        retry_text += f"  Time: {format_time(time.time() - data['timestamp'])} ago\n"
        
        # Show first 3 errors
        for error in data['errors'][:3]:
            retry_text += f"  • {error}\n"
        
        if len(data['errors']) > 3:
            retry_text += f"  ... and {len(data['errors']) - 3} more\n"
        retry_text += "\n"
    
    total_failed = sum(len(data['files']) for data in FAILED_UPLOADS.values())
    retry_text += f"**Total:** {total_failed} failed files across {len(FAILED_UPLOADS)} tasks\n\n"
    retry_text += "⚠️ Automatic retry is now built-in!\n"
    retry_text += f"Failed uploads are retried up to {MAX_RETRIES} times automatically.\n\n"
    retry_text += "Use `/clearfailed` to clear failed uploads history"
    
    await message.reply_text(retry_text)

@app.on_message(filters.command("clearfailed") & filters.user(OWNER_ID))
async def clearfailed_command(client, message):
    """Clear failed uploads history"""
    if not FAILED_UPLOADS:
        await message.reply_text("ℹ️ No failed uploads to clear")
        return
    
    count = len(FAILED_UPLOADS)
    FAILED_UPLOADS.clear()
    await message.reply_text(f"🗑️ Cleared {count} failed upload records")

@app.on_message(filters.command("browse") & filters.user(OWNER_ID))
async def browse_command(client, message):
    """Browse Google Drive files"""
    try:
        service = get_drive_service()
        if not service:
            await message.reply_text("❌ Failed to connect to Google Drive")
            return
        
        user_id = message.from_user.id
        session = get_browser_session(user_id)
        
        # Reset to root
        session['current_folder'] = DRIVE_FOLDER_ID
        session['path'] = [{'name': 'My Drive', 'id': DRIVE_FOLDER_ID}]
        session['selected_files'] = []
        session['page'] = 0
        
        # List files
        folders, files, _ = list_drive_files(service, DRIVE_FOLDER_ID)
        total_items = len(folders) + len(files)
        
        if total_items == 0:
            await message.reply_text("📁 Folder is empty")
            return
        
        # Build keyboard
        keyboard = build_browser_keyboard(user_id, folders, files, total_items)
        breadcrumb = get_breadcrumb(session)
        
        await message.reply_text(
            f"📂 **Drive Browser**\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📍 {breadcrumb}\n"
            f"📊 {len(folders)} folders • {len(files)} files",
            reply_markup=keyboard
        )
    
    except Exception as e:
        logger.error(f"Browse error: {e}")
        await message.reply_text(f"❌ Error: {str(e)}")

@app.on_message(filters.command("search") & filters.user(OWNER_ID))
async def search_command(client, message):
    """Search Google Drive"""
    try:
        # Extract search query
        query = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else None
        
        if not query:
            await message.reply_text("🔍 **Search Drive**\n\nUsage: `/search <query>`\n\nExample: `/search Harry Potter`")
            return
        
        service = get_drive_service()
        if not service:
            await message.reply_text("❌ Failed to connect to Google Drive")
            return
        
        status = await message.reply_text(f"🔍 Searching for: `{query}`...")
        
        # Search
        folders, files = search_drive_files(service, query)
        total_results = len(folders) + len(files)
        
        if total_results == 0:
            await status.edit_text(f"🔍 No results found for: `{query}`")
            return
        
        # Build keyboard for results
        user_id = message.from_user.id
        session = get_browser_session(user_id)
        session['page'] = 0
        session['selected_files'] = []
        
        keyboard = build_browser_keyboard(user_id, folders, files, total_results)
        
        await status.edit_text(
            f"🔍 **Search Results**\n"
            f"━━━━━━━━━━━━━━━\n"
            f"Query: `{query}`\n"
            f"📊 {len(folders)} folders • {len(files)} files",
            reply_markup=keyboard
        )
    
    except IndexError:
        await message.reply_text("🔍 **Search Drive**\n\nUsage: `/search <query>`\n\nExample: `/search Harry Potter`")
    except Exception as e:
        logger.error(f"Search error: {e}")
        await message.reply_text(f"❌ Error: {str(e)}")

@app.on_message(filters.media & filters.user(OWNER_ID))
async def handle_media(client, message: Message):
    """Handle incoming media files"""
    try:
        # Get file object and name
        media_type = message.media.value
        file_obj = getattr(message, media_type)
        filename = getattr(file_obj, 'file_name', None)
        caption = message.caption or ""  # Capture caption
        
        if not filename:
            # Generate filename for media without names
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            ext_map = {'photo': 'jpg', 'video': 'mp4', 'audio': 'mp3', 'voice': 'ogg', 'animation': 'gif'}
            ext = ext_map.get(media_type, 'file')
            filename = f"{media_type}_{timestamp}.{ext}"
        
        # Info object
        file_info = {'msg_id': message.id, 'name': filename, 'caption': caption}

        # Check if part of media group (album)
        if message.media_group_id:
            group_id = message.media_group_id
            
            if group_id not in ALBUMS:
                ALBUMS[group_id] = []
                
                # Wait for all album files (2 second delay)
                async def process_album():
                    await asyncio.sleep(2)
                    
                    if group_id in ALBUMS:
                        file_list = ALBUMS.pop(group_id)
                        key = f"album_{group_id}"
                        TEMP_FILES[key] = file_list
                        
                        # Check for ANY caption in the album
                        detected_caption = next((f['caption'] for f in file_list if f['caption']), None)
                        first_file = file_list[0]['name']
                        cleaned_name = clean_series_name(first_file)
                        
                        buttons = []
                        # 1. Series (Caption OR Auto) - Priority to Caption
                        if detected_caption:
                            buttons.append([InlineKeyboardButton(f"📂 Series: {detected_caption[:30]}...", callback_data=f"cap|{key}")])
                        else:
                            buttons.append([InlineKeyboardButton(f"📂 Series: {cleaned_name[:30]}...", callback_data=f"auto|{key}")])
                        
                        # 2. Standalone
                        buttons.append([InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")])
                        # 3. Not an Audiobook
                        buttons.append([InlineKeyboardButton("🚫 Not an Audiobook", callback_data=f"root|{key}")])
                        # 4. Custom
                        buttons.append([InlineKeyboardButton("✏️ Custom Series Name", callback_data=f"custom|{key}")])
                        # 5. Cancel
                        buttons.append([InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_selection|{key}")])

                        txt = f"📦 **Album Detected**\n📚 Files: {len(file_list)}"
                        if detected_caption: txt += f"\n🏷 **Caption:** `{detected_caption}`"
                        await message.reply_text(txt, reply_markup=InlineKeyboardMarkup(buttons))
                
                asyncio.create_task(process_album())
            
            ALBUMS[group_id].append(file_info)
        
        else:
            # Single file
            key = f"single_{message.id}"
            TEMP_FILES[key] = [file_info]
            cleaned_name = clean_series_name(filename)
            
            buttons = []
            # 1. Series (Caption OR Auto)
            if caption:
                buttons.append([InlineKeyboardButton(f"📂 Series: {caption[:30]}...", callback_data=f"cap|{key}")])
            else:
                buttons.append([InlineKeyboardButton(f"📂 Series: {cleaned_name[:30]}...", callback_data=f"auto|{key}")])
            
            # 2. Standalone
            buttons.append([InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")])
            # 3. Custom Series Name
            buttons.append([InlineKeyboardButton("✏️ Custom Series Name", callback_data=f"custom|{key}")])
            # 4. Not an Audiobook
            buttons.append([InlineKeyboardButton("🚫 Not an Audiobook", callback_data=f"root|{key}")])
            # 5. Cancel
            buttons.append([InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_selection|{key}")])
            
            txt = f"📄 **File Received**\n`{filename}`"
            if caption: txt += f"\n🏷 **Caption:** `{caption}`"
            await message.reply_text(txt, reply_markup=InlineKeyboardMarkup(buttons))
    
    except Exception as e:
        logger.error(f"Error handling media: {e}")
        await message.reply_text(f"❌ Error: {str(e)}")

@app.on_callback_query()
async def handle_callback(client, query):
    """Handle button callbacks"""
    try:
        # ==================== BROWSER CALLBACKS ====================
        if query.data.startswith("browser_"):
            user_id = query.from_user.id
            service = get_drive_service()
            
            if not service:
                await query.answer("❌ Failed to connect to Drive", show_alert=True)
                return
            
            session = get_browser_session(user_id)
            
            # Open folder
            if query.data.startswith("browser_open|"):
                folder_id = query.data.replace("browser_open|", "")
                
                # Get folder info
                folder_info = get_folder_info(service, folder_id)
                if not folder_info:
                    await query.answer("❌ Folder not found", show_alert=True)
                    return
                
                # FIXED: Save current page before navigating
                current_page = session.get('page', 0)
                
                # Update session
                session['current_folder'] = folder_id
                session['path'].append({
                    'name': folder_info['name'], 
                    'id': folder_id,
                    'parent_page': current_page  # SAVE PAGE NUMBER
                })
                session['page'] = 0  # Reset to page 0 in new folder
                session['selected_files'] = []
                
                # List files in folder
                folders, files, _ = list_drive_files(service, folder_id)
                total_items = len(folders) + len(files)
                
                if total_items == 0:
                    await query.answer("📁 Folder is empty", show_alert=True)
                    # Go back
                    session['path'].pop()
                    if session['path']:
                        session['current_folder'] = session['path'][-1]['id']
                    return
                
                # Build keyboard
                keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                breadcrumb = get_breadcrumb(session)
                
                await query.message.edit_text(
                    f"📁 **File Browser**\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(folders)} folders, {len(files)} files",
                    reply_markup=keyboard
                )
                await query.answer()
            
            # Select/Deselect file
            elif query.data.startswith("browser_select|"):
                file_id = query.data.replace("browser_select|", "")
                
                # Toggle selection
                if file_id in session['selected_files']:
                    session['selected_files'].remove(file_id)
                    action = "Deselected"
                else:
                    session['selected_files'].append(file_id)
                    action = "Selected"
                
                # Quick answer without alert
                await query.answer(f"✅ {action}", show_alert=False)
                
                # Optimized refresh - handle FloodWait and MessageNotModified gracefully
                try:
                    folders, files, _ = list_drive_files(service, session['current_folder'])
                    total_items = len(folders) + len(files)
                    keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                    breadcrumb = get_breadcrumb(session)
                    
                    await safe_edit_message(
                        query.message,
                        f"📁 **File Browser**\n"
                        f"📍 {breadcrumb}\n"
                        f"📊 {len(folders)} folders, {len(files)} files\n"
                        f"✅ Selected: {len(session['selected_files'])}",
                        reply_markup=keyboard
                    )
                except FloodWait as e:
                    logger.warning(f"FloodWait on selection: {e.value}s - selection saved")
                    await asyncio.sleep(e.value)
                except Exception as e:
                    logger.debug(f"UI refresh error (non-critical): {e}")
            
            # File info
            elif query.data.startswith("browser_info|"):
                file_id = query.data.replace("browser_info|", "")
                
                try:
                    file_info = service.files().get(
                        fileId=file_id,
                        fields="id, name, mimeType, size, modifiedTime, createdTime"
                    ).execute()
                    
                    name = file_info['name']
                    size = format_size(int(file_info.get('size', 0))) if 'size' in file_info else 'N/A'
                    modified = file_info.get('modifiedTime', 'N/A')[:10]
                    created = file_info.get('createdTime', 'N/A')[:10]
                    
                    info_text = (
                        f"📄 **File Info**\n\n"
                        f"**Name:** `{name}`\n"
                        f"**Size:** {size}\n"
                        f"**Modified:** {modified}\n"
                        f"**Created:** {created}\n"
                    )
                    
                    await query.answer(info_text, show_alert=True)
                except Exception as e:
                    await query.answer(f"❌ Error: {str(e)[:100]}", show_alert=True)
            
            # Download selected
            elif query.data == "browser_download":
                if not session['selected_files']:
                    await query.answer("❌ No files selected", show_alert=True)
                    return
                
                file_count = len(session['selected_files'])
                await query.message.edit_text(f"📥 **Starting download...**\n\n📦 Files: {file_count}")
                
                # Start download task
                asyncio.create_task(download_from_drive_task(
                    client, 
                    query.message, 
                    session['selected_files'].copy(), 
                    service
                ))
                
                # Clear selection
                session['selected_files'] = []
                await query.answer(f"📥 Downloading {file_count} files...", show_alert=False)
            
            # Upload to Telegram
            elif query.data == "browser_upload":
                if not session['selected_files']:
                    await query.answer("❌ No files selected", show_alert=True)
                    return
                
                # Separate folders and files
                selected_folders = []
                selected_files = []
                
                for item_id in session['selected_files']:
                    try:
                        metadata = service.files().get(fileId=item_id, fields="id, name, mimeType, size").execute()
                        if metadata['mimeType'] == 'application/vnd.google-apps.folder':
                            selected_folders.append(metadata)
                        else:
                            selected_files.append(metadata)
                    except Exception as e:
                        logger.error(f"Error getting metadata for {item_id}: {e}")
                
                # Validate folders - check for subfolders and file count
                valid_folders = []
                invalid_folders = []
                folder_warnings = []
                
                for folder in selected_folders:
                    try:
                        subfolders, files_in_folder, _ = list_drive_files(service, folder['id'])
                        
                        # Check for subfolders
                        if len(subfolders) > 0:
                            invalid_folders.append(f"{folder['name']} (has {len(subfolders)} subfolder(s))")
                            logger.warning(f"Folder '{folder['name']}' has {len(subfolders)} subfolders - rejected")
                            continue
                        
                        # Check if folder has files
                        if len(files_in_folder) == 0:
                            folder_warnings.append(f"{folder['name']} (empty)")
                            logger.warning(f"Folder '{folder['name']}' is empty - skipped")
                            continue
                        
                        # Folder is valid
                        valid_folders.append(folder)
                        logger.info(f"Folder '{folder['name']}' validated: {len(files_in_folder)} files")
                        
                    except Exception as e:
                        logger.error(f"Error validating folder '{folder['name']}': {e}")
                        invalid_folders.append(f"{folder['name']} (error: {str(e)[:30]})")
                
                # Show detailed error if folders were rejected
                if invalid_folders:
                    error_msg = "❌ **Cannot upload these folders:**\n\n"
                    for invalid in invalid_folders[:5]:
                        error_msg += f"• {invalid}\n"
                    error_msg += "\n💡 **Tip:** Folders with subfolders cannot be uploaded. "
                    error_msg += "Open the folder and select files directly, or flatten the folder structure first."
                    
                    await query.answer("Check message for details", show_alert=True)
                    await query.message.reply_text(error_msg)
                    return
                
                # Show warning for empty folders but continue
                if folder_warnings and not valid_folders and not selected_files:
                    await query.answer("❌ All selected folders are empty", show_alert=True)
                    return
                
                total_items = len(valid_folders) + len(selected_files)
                
                if total_items == 0:
                    await query.answer("❌ No valid items to upload", show_alert=True)
                    return
                
                # Show status with warnings if any
                status_text = f"📤 **Uploading to Telegram**\n\n"
                status_text += f"📁 Folders: {len(valid_folders)}\n"
                status_text += f"📄 Files: {len(selected_files)}\n"
                status_text += f"📦 Total items: {total_items}\n"
                
                if folder_warnings:
                    status_text += f"\n⚠️ Skipped empty folders: {len(folder_warnings)}\n"
                
                status_text += "\n⏳ Starting..."
                
                await query.message.edit_text(status_text)
                
                # Start upload task
                asyncio.create_task(upload_to_telegram_task(
                    client,
                    query.message,
                    valid_folders,
                    selected_files,
                    service
                ))
                
                # Clear selection
                session['selected_files'] = []
                await query.answer(f"📤 Uploading {total_items} items...", show_alert=False)
            
            # Clear selection
            elif query.data == "browser_clear":
                session['selected_files'] = []
                
                # Refresh display
                folders, files, _ = list_drive_files(service, session['current_folder'])
                total_items = len(folders) + len(files)
                keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                breadcrumb = get_breadcrumb(session)
                
                await query.message.edit_text(
                    f"📁 **File Browser**\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(folders)} folders, {len(files)} files",
                    reply_markup=keyboard
                )
                await query.answer("✅ Selection cleared", show_alert=False)
            
            # NEW: Select all files on current page
            # MERGED: Select ALL items (files + folders) in current folder
            elif query.data == "browser_select_all":
                folders, files, _ = list_drive_files(service, session['current_folder'])
                
                # Select EVERYTHING - both files and folders
                all_items = folders + files
                for item in all_items:
                    if item['id'] not in session['selected_files']:
                        session['selected_files'].append(item['id'])
                
                total_items = len(folders) + len(files)
                keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                breadcrumb = get_breadcrumb(session)
                
                await safe_edit_message(
                    query.message,
                    f"📁 **File Browser**\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(folders)} folders, {len(files)} files",
                    reply_markup=keyboard
                )
                await query.answer(f"✅ Selected {len(all_items)} items", show_alert=False)
            
            # Back button
            elif query.data == "browser_back":
                if len(session['path']) > 1:
                    # Pop current folder
                    popped = session['path'].pop()
                    
                    # Go to parent folder
                    session['current_folder'] = session['path'][-1]['id']
                    
                    # FIXED: Restore the page we were on before entering this folder
                    session['page'] = popped.get('parent_page', 0)
                    
                    session['selected_files'] = []
                    
                    # List files
                    folders, files, _ = list_drive_files(service, session['current_folder'])
                    total_items = len(folders) + len(files)
                    keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                    breadcrumb = get_breadcrumb(session)
                    
                    await query.message.edit_text(
                        f"📁 **File Browser**\n"
                        f"📍 {breadcrumb}\n"
                        f"📊 {len(folders)} folders, {len(files)} files",
                        reply_markup=keyboard
                    )
                await query.answer()
            
            # Home button
            elif query.data == "browser_home":
                session['current_folder'] = DRIVE_FOLDER_ID
                session['path'] = [{'name': 'My Drive', 'id': DRIVE_FOLDER_ID}]
                session['page'] = 0
                session['selected_files'] = []
                
                # List files
                folders, files, _ = list_drive_files(service, DRIVE_FOLDER_ID)
                total_items = len(folders) + len(files)
                keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                breadcrumb = get_breadcrumb(session)
                
                await query.message.edit_text(
                    f"📁 **File Browser**\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(folders)} folders, {len(files)} files",
                    reply_markup=keyboard
                )
                await query.answer()
            
            # Pagination
            elif query.data == "browser_prev":
                session['page'] = max(0, session['page'] - 1)
                
                folders, files, _ = list_drive_files(service, session['current_folder'])
                total_items = len(folders) + len(files)
                keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                breadcrumb = get_breadcrumb(session)
                
                await query.message.edit_text(
                    f"📁 **File Browser**\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(folders)} folders, {len(files)} files",
                    reply_markup=keyboard
                )
                await query.answer()
            
            elif query.data == "browser_next":
                folders, files, _ = list_drive_files(service, session['current_folder'])
                total_items = len(folders) + len(files)
                max_page = (total_items - 1) // ITEMS_PER_PAGE
                
                session['page'] = min(max_page, session['page'] + 1)
                
                keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                breadcrumb = get_breadcrumb(session)
                
                await query.message.edit_text(
                    f"📁 **File Browser**\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(folders)} folders, {len(files)} files",
                    reply_markup=keyboard
                )
                await query.answer()
            
            # Refresh
            elif query.data == "browser_refresh":
                folders, files, _ = list_drive_files(service, session['current_folder'])
                total_items = len(folders) + len(files)
                keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                breadcrumb = get_breadcrumb(session)
                
                await query.message.edit_text(
                    f"📁 **File Browser**\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(folders)} folders, {len(files)} files",
                    reply_markup=keyboard
                )
                await query.answer("🔄 Refreshed", show_alert=False)
            
            # No-op handler for non-interactive buttons (like page indicator)
            elif query.data == "browser_noop":
                await query.answer()
            
            # Search button
            elif query.data == "browser_search":
                await query.answer("🔍 Use /search <query> command", show_alert=True)
            
            # Close browser
            elif query.data == "browser_close":
                # Clean up session
                if user_id in BROWSER_SESSIONS:
                    del BROWSER_SESSIONS[user_id]
                
                await query.message.edit_text(
                    "✅ **Browser Closed**\n\n"
                    "Use /browse to open again."
                )
                await query.answer("Browser closed", show_alert=False)
            
            return
        
        # ==================== NEW: FOLDER SELECTION CALLBACKS FOR UPLOAD ====================
        # Handle folder browsing during upload destination selection
        if query.data.startswith("upload_browse|"):
            folder_id = query.data.replace("upload_browse|", "")
            user_id = query.from_user.id
            
            if user_id not in PENDING_FOLDER_SELECTION:
                await query.answer("❌ Session expired", show_alert=True)
                return
            
            service = get_drive_service()
            if not service:
                await query.answer("❌ Failed to connect to Drive", show_alert=True)
                return
            
            # List folders in selected folder
            folders, _, _ = list_drive_files(service, folder_id)
            keyboard = build_folder_selection_keyboard(user_id, folders, folder_id)
            
            pending = PENDING_FOLDER_SELECTION[user_id]
            mode_text = pending.get('mode', 'upload')
            series_name = pending.get('series_name', '')
            file_count = len(pending.get('file_list', []))
            
            # Get folder info for breadcrumb
            try:
                folder_info = service.files().get(
                    fileId=folder_id,
                    fields="name",
                    supportsAllDrives=True
                ).execute()
                folder_name = folder_info['name']
            except:
                folder_name = "Folder"
            
            if mode_text == 'series' and series_name:
                message_text = (
                    f"📁 **Select Upload Destination**\n\n"
                    f"Current: {folder_name}\n"
                    f"Series: **{series_name}**\n"
                    f"Files: {file_count}\n\n"
                    f"Choose where to create '{series_name}' folder:"
                )
            elif mode_text == 'standalone':
                message_text = (
                    f"📁 **Select Upload Destination**\n\n"
                    f"Current: {folder_name}\n"
                    f"Mode: Standalone\n"
                    f"Files: {file_count}\n\n"
                    f"Choose where to create folders:"
                )
            else:
                message_text = (
                    f"📁 **Select Upload Destination**\n\n"
                    f"Current: {folder_name}\n"
                    f"Files: {file_count}\n\n"
                    f"Choose destination folder:"
                )
            
            await query.message.edit_text(message_text, reply_markup=keyboard)
            await query.answer()
            return
        
        # Handle back button in folder selection
        if query.data == "upload_folder_back":
            # For now, just go to root (can enhance later with breadcrumb tracking)
            user_id = query.from_user.id
            
            if user_id not in PENDING_FOLDER_SELECTION:
                await query.answer("❌ Session expired", show_alert=True)
                return
            
            service = get_drive_service()
            if not service:
                await query.answer("❌ Failed to connect to Drive", show_alert=True)
                return
            
            folders, _, _ = list_drive_files(service, DRIVE_FOLDER_ID)
            keyboard = build_folder_selection_keyboard(user_id, folders, DRIVE_FOLDER_ID)
            
            pending = PENDING_FOLDER_SELECTION[user_id]
            await query.message.edit_text(
                f"📁 **Select Upload Destination**\n\n"
                f"Back to Root\n"
                f"Files: {len(pending.get('file_list', []))}\n\n"
                f"Choose destination:",
                reply_markup=keyboard
            )
            await query.answer()
            return
        
        # Handle home button in folder selection
        if query.data == "upload_folder_home":
            user_id = query.from_user.id
            
            if user_id not in PENDING_FOLDER_SELECTION:
                await query.answer("❌ Session expired", show_alert=True)
                return
            
            service = get_drive_service()
            if not service:
                await query.answer("❌ Failed to connect to Drive", show_alert=True)
                return
            
            folders, _, _ = list_drive_files(service, DRIVE_FOLDER_ID)
            keyboard = build_folder_selection_keyboard(user_id, folders, DRIVE_FOLDER_ID)
            
            pending = PENDING_FOLDER_SELECTION[user_id]
            await query.message.edit_text(
                f"📁 **Select Upload Destination**\n\n"
                f"Location: Root\n"
                f"Files: {len(pending.get('file_list', []))}\n\n"
                f"Choose destination:",
                reply_markup=keyboard
            )
            await query.answer()
            return
        
        # Handle folder confirmation - START UPLOAD
        if query.data.startswith("upload_confirm_folder|"):
            selected_folder_id = query.data.replace("upload_confirm_folder|", "")
            user_id = query.from_user.id
            
            if user_id not in PENDING_FOLDER_SELECTION:
                await query.answer("❌ Session expired", show_alert=True)
                return
            
            pending = PENDING_FOLDER_SELECTION[user_id]
            file_list = pending['file_list']
            series_name = pending.get('series_name')
            flat_upload = pending.get('flat_upload', False)
            key = pending.get('key')
            
            # Clean up
            del PENDING_FOLDER_SELECTION[user_id]
            if key and key in TEMP_FILES:
                del TEMP_FILES[key]
            
            # Start upload with selected folder as parent
            queue_id = add_to_queue(file_list, series_name=series_name, flat_upload=flat_upload)
            
            if series_name:
                await query.message.edit_text(
                    f"🚀 **Starting Upload**\n\n"
                    f"Series: **{series_name}**\n"
                    f"Files: {len(file_list)}\n"
                    f"📋 Queue ID: `{queue_id}`"
                )
            else:
                await query.message.edit_text(
                    f"🚀 **Starting Upload**\n\n"
                    f"Files: {len(file_list)}\n"
                    f"📋 Queue ID: `{queue_id}`"
                )
            
            asyncio.create_task(upload_task(
                client, 
                query.message, 
                file_list, 
                series_name=series_name, 
                flat_upload=flat_upload, 
                queue_id=queue_id,
                parent_folder_id=selected_folder_id  # NEW: Pass selected folder
            ))
            await query.answer()
            return
        
        
        # NEW: Navigate into folder (upload_nav)
        if query.data.startswith("upload_nav|"):
            folder_id = query.data.replace("upload_nav|", "")
            user_id = query.from_user.id
            
            if user_id not in PENDING_FOLDER_SELECTION:
                await query.answer("❌ Session expired", show_alert=True)
                return
            
            service = get_drive_service()
            if not service:
                await query.answer("❌ Failed to connect to Drive", show_alert=True)
                return
            
            # Update session
            session = BROWSER_SESSIONS[user_id]
            
            # Get folder info
            try:
                folder_info = service.files().get(
                    fileId=folder_id,
                    fields="id, name",
                    supportsAllDrives=True
                ).execute()
                
                # FIXED: Save current page before navigating
                current_page = session.get('page', 0)
                
                session['path'].append({
                    'id': session['current_folder'], 
                    'name': folder_info['name'],
                    'parent_page': current_page  # SAVE PAGE
                })
                session['current_folder'] = folder_id
                session['page'] = 0  # Reset to page 0 in new folder
            except Exception as e:
                await query.answer(f"❌ Error: {str(e)}", show_alert=True)
                return
            
            # List folders
            folders, _, _ = list_drive_files(service, folder_id)
            keyboard = build_folder_selection_keyboard(user_id, folders, folder_id)
            
            pending = PENDING_FOLDER_SELECTION[user_id]
            breadcrumb = get_breadcrumb(session)
            
            if pending.get('mode') == 'series' and pending.get('series_name'):
                text = (
                    f"📁 **Select Upload Destination**\n\n"
                    f"📍 {breadcrumb}\n"
                    f"📚 Series: {pending['series_name']}\n"
                    f"📊 {len(pending['file_list'])} file(s)\n\n"
                    f"Choose where to create series folder:"
                )
            else:
                text = (
                    f"📁 **Select Upload Destination**\n\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(pending['file_list'])} file(s)\n\n"
                    f"Choose destination folder:"
                )
            
            await query.message.edit_text(text, reply_markup=keyboard)
            await query.answer()
            return
        
        # NEW: Select this folder for upload (upload_select)
        if query.data.startswith("upload_select|"):
            selected_folder_id = query.data.replace("upload_select|", "")
            user_id = query.from_user.id
            
            if user_id not in PENDING_FOLDER_SELECTION:
                await query.answer("❌ Session expired", show_alert=True)
                return
            
            pending = PENDING_FOLDER_SELECTION[user_id]
            file_list = pending['file_list']
            series_name = pending.get('series_name')
            flat_upload = pending.get('flat_upload', False)
            key = pending.get('key')
            
            # Get breadcrumb for confirmation
            session = BROWSER_SESSIONS[user_id]
            breadcrumb = get_breadcrumb(session)
            
            # Clean up
            del PENDING_FOLDER_SELECTION[user_id]
            if key and key in TEMP_FILES:
                del TEMP_FILES[key]
            
            # Start upload with selected folder as parent
            queue_id = add_to_queue(file_list, series_name=series_name, flat_upload=flat_upload)
            
            if series_name:
                await query.message.edit_text(
                    f"🚀 **Starting Upload**\n\n"
                    f"📚 Series: **{series_name}**\n"
                    f"📍 Destination: {breadcrumb}\n"
                    f"📊 Files: {len(file_list)}\n"
                    f"📋 Queue ID: `{queue_id}`"
                )
            else:
                mode_desc = "Direct (no subfolders)" if flat_upload else "Standalone"
                await query.message.edit_text(
                    f"🚀 **Starting Upload**\n\n"
                    f"🎯 Mode: {mode_desc}\n"
                    f"📍 Destination: {breadcrumb}\n"
                    f"📊 Files: {len(file_list)}\n"
                    f"📋 Queue ID: `{queue_id}`"
                )
            
            asyncio.create_task(upload_task(
                client, 
                query.message, 
                file_list, 
                series_name=series_name, 
                flat_upload=flat_upload, 
                queue_id=queue_id,
                parent_folder_id=selected_folder_id  # Pass selected folder
            ))
            await query.answer("✅ Upload started!", show_alert=False)
            return
        
        # NEW: Go back one folder (upload_back)
        if query.data == "upload_back":
            user_id = query.from_user.id
            
            if user_id not in PENDING_FOLDER_SELECTION:
                await query.answer("❌ Session expired", show_alert=True)
                return
            
            session = BROWSER_SESSIONS[user_id]
            
            if session['path'] and len(session['path']) > 1:
                # Pop current folder
                popped = session['path'].pop()
                
                # Get parent folder
                prev_folder = session['path'][-1]
                session['current_folder'] = prev_folder['id']
                
                # FIXED: Restore the page we were on
                session['page'] = popped.get('parent_page', 0)
                
                service = get_drive_service()
                if not service:
                    await query.answer("❌ Failed to connect to Drive", show_alert=True)
                    return
                
                folders, _, _ = list_drive_files(service, prev_folder['id'])
                keyboard = build_folder_selection_keyboard(user_id, folders, prev_folder['id'])
                
                pending = PENDING_FOLDER_SELECTION[user_id]
                breadcrumb = get_breadcrumb(session)
                
                await query.message.edit_text(
                    f"📁 **Select Upload Destination**\n\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(pending['file_list'])} file(s)",
                    reply_markup=keyboard
                )
                await query.answer()
            else:
                await query.answer("Already at root", show_alert=False)
            return
        
        # NEW: Previous page (upload_prev)
        if query.data == "upload_prev":
            user_id = query.from_user.id
            
            if user_id not in PENDING_FOLDER_SELECTION:
                await query.answer("❌ Session expired", show_alert=True)
                return
            
            session = BROWSER_SESSIONS[user_id]
            session['page'] = max(0, session['page'] - 1)
            
            service = get_drive_service()
            if not service:
                await query.answer("❌ Failed to connect to Drive", show_alert=True)
                return
            
            folders, _, _ = list_drive_files(service, session['current_folder'])
            keyboard = build_folder_selection_keyboard(user_id, folders, session['current_folder'])
            
            pending = PENDING_FOLDER_SELECTION[user_id]
            breadcrumb = get_breadcrumb(session)
            
            await query.message.edit_text(
                f"📁 **Select Upload Destination**\n\n"
                f"📍 {breadcrumb}\n"
                f"📊 {len(pending['file_list'])} file(s)",
                reply_markup=keyboard
            )
            await query.answer()
            return
        
        # NEW: Next page (upload_next)
        if query.data == "upload_next":
            user_id = query.from_user.id
            
            if user_id not in PENDING_FOLDER_SELECTION:
                await query.answer("❌ Session expired", show_alert=True)
                return
            
            session = BROWSER_SESSIONS[user_id]
            
            service = get_drive_service()
            if not service:
                await query.answer("❌ Failed to connect to Drive", show_alert=True)
                return
            
            folders, _, _ = list_drive_files(service, session['current_folder'])
            max_page = (len(folders) - 1) // ITEMS_PER_PAGE
            session['page'] = min(max_page, session['page'] + 1)
            
            keyboard = build_folder_selection_keyboard(user_id, folders, session['current_folder'])
            
            pending = PENDING_FOLDER_SELECTION[user_id]
            breadcrumb = get_breadcrumb(session)
            
            await query.message.edit_text(
                f"📁 **Select Upload Destination**\n\n"
                f"📍 {breadcrumb}\n"
                f"📊 {len(pending['file_list'])} file(s)",
                reply_markup=keyboard
            )
            await query.answer()
            return
        
        # NEW: No-op for page indicator (upload_noop)
        if query.data == "upload_noop":
            await query.answer()
            return
        
        # Handle upload cancel
        if query.data == "upload_cancel":
            user_id = query.from_user.id
            
            if user_id in PENDING_FOLDER_SELECTION:
                pending = PENDING_FOLDER_SELECTION[user_id]
                key = pending.get('key')
                file_count = len(pending.get('file_list', []))
                
                del PENDING_FOLDER_SELECTION[user_id]
                if key and key in TEMP_FILES:
                    del TEMP_FILES[key]
                
                await query.message.edit_text(
                    f"❌ **Upload Cancelled**\n\n"
                    f"{file_count} file(s) discarded."
                )
            else:
                await query.message.edit_text("❌ **Upload Cancelled**")
            
            await query.answer()
            return
        
        # ==================== UPLOAD CALLBACKS ====================
        # Check for cancel selection button (before upload starts)
        if query.data.startswith("cancel_selection|"):
            key = query.data.replace("cancel_selection|", "")
            
            # Remove from temp files
            if key in TEMP_FILES:
                file_count = len(TEMP_FILES[key])
                del TEMP_FILES[key]
                logger.info(f"User cancelled selection for key: {key}")
                
                await query.answer("✅ Cancelled", show_alert=False)
                await query.message.edit_text(
                    f"❌ **Upload Cancelled**\n\n"
                    f"{file_count} file(s) discarded.\n"
                    f"Send new files to upload again."
                )
            else:
                await query.answer("ℹ️ Already processed", show_alert=True)
            return
        
        # Check for cancel button (during upload)
        if query.data.startswith("cancel_"):
            task_id = query.data.replace("cancel_", "")
            
            if task_id in ACTIVE_TASKS:
                ACTIVE_TASKS[task_id]['cancelled'] = True
                logger.info(f"Cancelling task via button: {task_id}")
                
                await query.answer("🛑 Cancellation requested...", show_alert=True)
                await query.message.edit_text(
                    f"🛑 **Cancellation Requested**\n\n"
                    f"Please wait for the task to stop..."
                )
            else:
                await query.answer("ℹ️ Task already completed", show_alert=True)
            return
        
        data_parts = query.data.split('|')
        mode = data_parts[0]
        key = data_parts[1] if len(data_parts) > 1 else None
        
        if not key or key not in TEMP_FILES:
            await query.answer("❌ Session expired. Please send the file again.", show_alert=True)
            return
        
        file_list = TEMP_FILES[key]
        
        if mode == "std":
            # Standalone mode - Show folder selector first
            user_id = query.from_user.id
            
            PENDING_FOLDER_SELECTION[user_id] = {
                'file_list': file_list,
                'series_name': None,
                'mode': 'standalone',
                'flat_upload': False,
                'key': key
            }
            
            # Initialize browser session
            if user_id not in BROWSER_SESSIONS:
                BROWSER_SESSIONS[user_id] = {
                    'current_folder': DRIVE_FOLDER_ID,
                    'path': [{'name': 'Root', 'id': DRIVE_FOLDER_ID}],
                    'selected_files': [],
                    'page': 0
                }
            
            session = BROWSER_SESSIONS[user_id]
            session['current_folder'] = DRIVE_FOLDER_ID
            session['path'] = [{'name': 'Root', 'id': DRIVE_FOLDER_ID}]
            session['page'] = 0
            
            # Show folder browser
            service = get_drive_service()
            if not service:
                await query.answer("❌ Drive connection failed", show_alert=True)
                return
            
            folders, _, _ = list_drive_files(service, DRIVE_FOLDER_ID)
            keyboard = build_folder_selection_keyboard(user_id, folders, DRIVE_FOLDER_ID)
            
            await query.message.edit_text(
                f"📁 **Select Upload Destination**\n\n"
                f"🎯 Mode: Standalone\n"
                f"📂 Each file gets its own folder\n"
                f"📊 {len(file_list)} file(s)\n\n"
                f"Choose destination folder:",
                reply_markup=keyboard
            )
            await query.answer()
        
        elif mode == "root":
            # Root mode - Show folder selector
            user_id = query.from_user.id
            
            PENDING_FOLDER_SELECTION[user_id] = {
                'file_list': file_list,
                'series_name': None,
                'mode': 'root',
                'flat_upload': True,
                'key': key
            }
            
            # Initialize browser session
            if user_id not in BROWSER_SESSIONS:
                BROWSER_SESSIONS[user_id] = {
                    'current_folder': DRIVE_FOLDER_ID,
                    'path': [{'name': 'Root', 'id': DRIVE_FOLDER_ID}],
                    'selected_files': [],
                    'page': 0
                }
            
            session = BROWSER_SESSIONS[user_id]
            session['current_folder'] = DRIVE_FOLDER_ID
            session['path'] = [{'name': 'Root', 'id': DRIVE_FOLDER_ID}]
            session['page'] = 0
            
            # Show folder browser
            service = get_drive_service()
            if not service:
                await query.answer("❌ Drive connection failed", show_alert=True)
                return
            
            folders, _, _ = list_drive_files(service, DRIVE_FOLDER_ID)
            keyboard = build_folder_selection_keyboard(user_id, folders, DRIVE_FOLDER_ID)
            
            await query.message.edit_text(
                f"📁 **Select Upload Destination**\n\n"
                f"🎯 Mode: Direct Upload (No subfolders)\n"
                f"📊 {len(file_list)} file(s)\n\n"
                f"Choose destination folder:",
                reply_markup=keyboard
            )
            await query.answer()

        elif mode == "cap":
            # Caption mode - Show folder selector
            series_name = next((f['caption'] for f in file_list if f['caption']), "Unknown Series")
            user_id = query.from_user.id
            
            PENDING_FOLDER_SELECTION[user_id] = {
                'file_list': file_list,
                'series_name': series_name,
                'mode': 'series',
                'flat_upload': False,
                'key': key
            }
            
            # Initialize browser session
            if user_id not in BROWSER_SESSIONS:
                BROWSER_SESSIONS[user_id] = {
                    'current_folder': DRIVE_FOLDER_ID,
                    'path': [{'name': 'Root', 'id': DRIVE_FOLDER_ID}],
                    'selected_files': [],
                    'page': 0
                }
            
            session = BROWSER_SESSIONS[user_id]
            session['current_folder'] = DRIVE_FOLDER_ID
            session['path'] = [{'name': 'Root', 'id': DRIVE_FOLDER_ID}]
            session['page'] = 0
            
            # Show folder browser
            service = get_drive_service()
            if not service:
                await query.answer("❌ Drive connection failed", show_alert=True)
                return
            
            folders, _, _ = list_drive_files(service, DRIVE_FOLDER_ID)
            keyboard = build_folder_selection_keyboard(user_id, folders, DRIVE_FOLDER_ID)
            
            await query.message.edit_text(
                f"📁 **Select Upload Destination**\n\n"
                f"🎯 Mode: Series Upload\n"
                f"📚 Series: {series_name}\n"
                f"📊 {len(file_list)} file(s)\n\n"
                f"Choose where to create series folder:",
                reply_markup=keyboard
            )
            await query.answer()
        
        elif mode == "auto":
            # Auto mode - Show folder selector
            first_file = file_list[0]['name']
            series_name = clean_series_name(first_file)
            user_id = query.from_user.id
            
            PENDING_FOLDER_SELECTION[user_id] = {
                'file_list': file_list,
                'series_name': series_name,
                'mode': 'series',
                'flat_upload': False,
                'key': key
            }
            
            # Initialize browser session
            if user_id not in BROWSER_SESSIONS:
                BROWSER_SESSIONS[user_id] = {
                    'current_folder': DRIVE_FOLDER_ID,
                    'path': [{'name': 'Root', 'id': DRIVE_FOLDER_ID}],
                    'selected_files': [],
                    'page': 0
                }
            
            session = BROWSER_SESSIONS[user_id]
            session['current_folder'] = DRIVE_FOLDER_ID
            session['path'] = [{'name': 'Root', 'id': DRIVE_FOLDER_ID}]
            session['page'] = 0
            
            # Show folder browser
            service = get_drive_service()
            if not service:
                await query.answer("❌ Drive connection failed", show_alert=True)
                return
            
            folders, _, _ = list_drive_files(service, DRIVE_FOLDER_ID)
            keyboard = build_folder_selection_keyboard(user_id, folders, DRIVE_FOLDER_ID)
            
            await query.message.edit_text(
                f"📁 **Select Upload Destination**\n\n"
                f"🎯 Mode: Series Upload\n"
                f"📚 Series: {series_name}\n"
                f"📊 {len(file_list)} file(s)\n\n"
                f"Choose where to create series folder:",
                reply_markup=keyboard
            )
            await query.answer()
        
        elif mode == "custom":
            # Ask for custom series name (folder selection will show after)
            ACTIVE_SERIES[query.from_user.id] = {
                'file_list': file_list,
                'key': key,
                'needs_folder_selection': True  # Flag to show folder selector after name entry
            }
            await query.message.edit_text(
                "✏️ **Enter Custom Series Name**\n\n"
                "Reply with the series name you want to use.\n"
                "After that, you'll choose where to save it."
            )
        
        await query.answer()
    
    except MessageNotModified:
        # Message content is the same, just answer the callback
        await query.answer()
    except Exception as e:
        logger.error(f"Callback error: {str(e)}")
        logger.error(f"Callback data: {query.data}")
        logger.error(f"User: {query.from_user.id}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        try:
            await query.answer(f"❌ Error: {str(e)}", show_alert=True)
        except:
            pass  # If we can't answer, just log

@app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "stats", "cancel", "queue", "status", "retry", "clearfailed"]))
async def handle_text(client, message: Message):
    """Handle text messages (Google Drive links or series names)"""
    user_id = message.from_user.id
    text = message.text.strip()
    
    # ==================== GOOGLE DRIVE LINK DETECTION ====================
    if 'drive.google.com' in text:
        # Handle Google Drive download
        logger.info(f"Google Drive link detected from user {user_id}")
        
        # Extract file ID from URL
        file_id = extract_file_id_from_url(text)
        
        if not file_id:
            await message.reply_text("❌ Couldn't extract file ID from the link. Make sure it's a valid Google Drive link.")
            return
        
        # Get Drive service
        service = get_drive_service()
        if not service:
            await message.reply_text("❌ Failed to connect to Google Drive")
            return
        
        # Get file/folder metadata
        status_msg = await message.reply_text("🔍 Checking Drive link...")
        
        metadata = get_drive_file_metadata(service, file_id)
        if not metadata:
            # FIXED: Get bot email to show in error message
            bot_email = "Unknown"
            try:
                about = service.about().get(fields="user").execute()
                bot_email = about.get('user', {}).get('emailAddress', 'Unknown')
            except:
                pass
            
            await status_msg.edit_text(
                "❌ **Couldn't access this file/folder**\n\n"
                "**Possible reasons:**\n"
                "1. File doesn't exist or was deleted\n"
                "2. Bot doesn't have permission\n"
                "3. Link is private/restricted\n\n"
                "**Solution:**\n"
                f"Share the file/folder with: `{bot_email}`\n"
                "Give it **Editor** access (NOT Viewer)\n"
                "Wait 1-2 minutes after sharing, then try again"
            )
            return
        
        file_name = metadata['name']
        mime_type = metadata['mimeType']
        
        # Check if it's a folder
        if mime_type == 'application/vnd.google-apps.folder':
            await status_msg.edit_text(f"📁 **Folder Detected:** {file_name}\n\n🔍 Finding all files...")
            
            # Get all files in folder
            files = list_folder_files_recursive(service, file_id)
            
            if not files:
                await status_msg.edit_text(f"❌ No files found in folder: {file_name}")
                return
            
            total_files = len(files)
            await status_msg.edit_text(
                f"📁 **Folder:** {file_name}\n"
                f"📊 **Files found:** {total_files}\n\n"
                f"⏳ Starting download..."
            )
            
            # Download and send each file
            successful = 0
            failed = 0
            
            for idx, file_info in enumerate(files, 1):
                try:
                    # Update status every 5 files
                    if idx % 5 == 0 or idx == 1:
                        await status_msg.edit_text(
                            f"📁 **Folder:** {file_name}\n"
                            f"📊 Progress: {idx}/{total_files}\n"
                            f"📄 Current: `{file_info['name'][:40]}...`\n"
                            f"✅ Sent: {successful} | ❌ Failed: {failed}"
                        )
                    
                    # Download from Drive
                    local_path = download_file_from_drive(service, file_info['id'], file_info['name'])
                    
                    if not local_path:
                        failed += 1
                        continue
                    
                    # Send to Telegram based on file type
                    file_size = int(file_info.get('size', 0))
                    is_audio = local_path.lower().endswith(('.mp3', '.m4a', '.m4b', '.flac', '.wav', '.ogg', '.aac', '.opus'))
                    is_video = local_path.lower().endswith(('.mp4', '.mkv', '.avi', '.mov', '.webm'))
                    
                    # Set caption based on file type
                    if is_video:
                        caption = None  # No caption for videos
                    elif is_audio:
                        caption = f"📖 {file_name}"  # Book emoji with folder name for audio
                    else:
                        caption = f"📁 {file_name}\n📄 {file_info['name']}\n💾 {format_size(file_size)}"
                    
                    try:
                        if is_video:
                            await message.reply_video(local_path)
                        elif is_audio:
                            await message.reply_audio(local_path, caption=caption)
                        elif local_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                            await message.reply_photo(local_path, caption=caption)
                        else:
                            await message.reply_document(local_path, caption=caption)
                        
                        successful += 1
                    except Exception as e:
                        logger.error(f"Error sending {file_info['name']}: {e}")
                        failed += 1
                    
                    # Clean up
                    try:
                        os.remove(local_path)
                    except:
                        pass
                    
                except FloodWait as e:
                    logger.warning(f"FloodWait: {e.value} seconds")
                    await asyncio.sleep(e.value)
                except Exception as e:
                    logger.error(f"Error processing {file_info['name']}: {e}")
                    failed += 1
            
            # Final summary
            await status_msg.edit_text(
                f"✅ **Folder Complete:** {file_name}\n\n"
                f"📊 Total: {total_files}\n"
                f"✅ Sent: {successful}\n"
                f"❌ Failed: {failed}"
            )
        
        else:
            # Single file download
            file_size = metadata.get('size', 'Unknown')
            
            await status_msg.edit_text(
                f"📄 **File:** {file_name}\n"
                f"💾 **Size:** {format_size(file_size)}\n\n"
                f"⏳ Downloading from Drive..."
            )
            
            # Download from Drive
            local_path = download_file_from_drive(service, file_id, file_name)
            
            if not local_path:
                await status_msg.edit_text(f"❌ Failed to download: {file_name}")
                return
            
            await status_msg.edit_text(
                f"📄 **File:** {file_name}\n"
                f"💾 **Size:** {format_size(file_size)}\n\n"
                f"⏫ Uploading to Telegram..."
            )
            
            try:
                # Send to Telegram based on file type
                is_audio = local_path.lower().endswith(('.mp3', '.m4a', '.m4b', '.flac', '.wav', '.ogg', '.aac', '.opus'))
                is_video = local_path.lower().endswith(('.mp4', '.mkv', '.avi', '.mov', '.webm'))
                
                if is_video:
                    # No caption for videos
                    await message.reply_video(local_path)
                elif is_audio:
                    # Simple filename caption for standalone audio
                    await message.reply_audio(local_path, caption=file_name)
                elif local_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                    await message.reply_photo(local_path, caption=f"📄 {file_name}\n💾 {format_size(file_size)}")
                else:
                    await message.reply_document(local_path, caption=f"📄 {file_name}\n💾 {format_size(file_size)}")
                
                await status_msg.edit_text(
                    f"✅ **Sent:** {file_name}\n"
                    f"💾 **Size:** {format_size(file_size)}"
                )
                
            except Exception as e:
                logger.error(f"Error sending file: {e}")
                await status_msg.edit_text(f"❌ Failed to upload to Telegram: {str(e)}")
            
            finally:
                # Clean up
                try:
                    os.remove(local_path)
                except:
                    pass
        
        return
    
    # ==================== SERIES NAME HANDLER (ORIGINAL UPLOAD LOGIC) ====================
    # ==================== CUSTOM SERIES NAME HANDLER ====================
    if user_id in ACTIVE_SERIES:
        series_data = ACTIVE_SERIES[user_id]
        file_list = series_data['file_list']
        key = series_data['key']
        series_name = text
        
        # Clean up ACTIVE_SERIES
        del ACTIVE_SERIES[user_id]
        
        # MODIFIED: Don't delete TEMP_FILES yet, save to PENDING_FOLDER_SELECTION and show folder browser
        PENDING_FOLDER_SELECTION[user_id] = {
            'file_list': file_list,
            'series_name': series_name,
            'flat_upload': False,
            'key': key,
            'mode': 'series'
        }
        
        # Show folder selection browser
        service = get_drive_service()
        if not service:
            await message.reply_text("❌ Failed to connect to Google Drive")
            if key in TEMP_FILES:
                del TEMP_FILES[key]
            return
        
        folders, _, _ = list_drive_files(service, DRIVE_FOLDER_ID)
        keyboard = build_folder_selection_keyboard(user_id, folders, DRIVE_FOLDER_ID)
        
        await message.reply_text(
            f"📁 **Select Upload Destination**\n\n"
            f"Series: **{series_name}**\n"
            f"Files: {len(file_list)}\n\n"
            f"Choose where to create '{series_name}' folder:",
            reply_markup=keyboard
        )
        return

# ==================== MAIN ====================
if __name__ == "__main__":
    logger.info("🤖 Starting RxUploader...")
    
    # Create downloads directory
    os.makedirs("downloads", exist_ok=True)
    
    # Load stats from Drive
    try:
        service = get_drive_service()
        if service:
            sync_stats(service, save=False)
            
            # FIXED: Show bot email on startup for easy sharing
            try:
                about = service.about().get(fields="user").execute()
                bot_email = about.get('user', {}).get('emailAddress', 'Unknown')
                logger.info(f"📧 Bot Email: {bot_email}")
                logger.info(f"⚠️  Share files with this email for downloads!")
            except Exception as e:
                logger.error(f"Couldn't get bot email: {e}")
    except Exception as e:
        logger.error(f"Failed to load initial stats: {e}")
    
    logger.info("✅ Bot initialized successfully!")
    logger.info(f"👤 Owner ID: {OWNER_ID}")
    logger.info(f"📁 Drive Folder: {DRIVE_FOLDER_ID}")
    
    # Run bot
    app.run()
