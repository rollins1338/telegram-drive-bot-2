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

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== HEALTH CHECK SERVER ====================
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
ACTIVE_TASKS = {}

# Queue Management
UPLOAD_QUEUE = {}
QUEUE_COUNTER = 0

# Error Recovery
FAILED_UPLOADS = {}
MAX_RETRIES = 3
RETRY_DELAY = 5

# File Browser State
BROWSER_SESSIONS = {}
DOWNLOAD_QUEUE = {}
FAVORITES = {}

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
        logger.error(f"Invalid TOKEN_JSON format: {e}")
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

def get_or_create_folder(service, folder_name, parent_id):
    """Find existing folder or create new one - FIXED VERSION"""
    try:
        # Sanitize folder name
        clean_name = folder_name.replace('"', '\\"')
        
        # FIXED: More comprehensive search that includes supportsAllDrives
        # This will find folders even if they were created outside the bot
        query = f'name="{clean_name}" and "{parent_id}" in parents and mimeType="application/vnd.google-apps.folder" and trashed=false'
        
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            spaces='drive',
            supportsAllDrives=True,  # ADDED: Support shared drives
            includeItemsFromAllDrives=True  # ADDED: Include items from all drives
        ).execute()
        
        folders = results.get('files', [])
        
        if folders:
            logger.info(f"📁 Found existing folder: {folder_name}")
            return folders[0]['id']
        
        # Create new folder if not found
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        
        folder = service.files().create(
            body=folder_metadata,
            fields='id',
            supportsAllDrives=True  # ADDED
        ).execute()
        
        logger.info(f"📁 Created new folder: {folder_name}")
        return folder['id']
    
    except Exception as e:
        logger.error(f"Error in get_or_create_folder: {e}")
        raise

def list_drive_files(service, folder_id=None, search_query=None):
    """List files and folders - FIXED VERSION"""
    try:
        if folder_id is None:
            folder_id = DRIVE_FOLDER_ID
        
        folders = []
        files = []
        
        # FIXED: Build query with proper Drive API parameters
        if search_query:
            # Search across ALL files in the main folder and subfolders
            query = f"name contains '{search_query}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false"
        else:
            # List direct children of specified folder
            query = f"'{folder_id}' in parents and trashed=false"
        
        # FIXED: Added pagination and proper Drive API parameters
        page_token = None
        while True:
            results = service.files().list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, webViewLink)",
                pageSize=100,  # Increased from default
                spaces='drive',
                supportsAllDrives=True,  # CRITICAL FIX
                includeItemsFromAllDrives=True,  # CRITICAL FIX
                pageToken=page_token,
                orderBy='folder,name'  # Folders first, then alphabetical
            ).execute()
            
            items = results.get('files', [])
            
            for item in items:
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    folders.append(item)
                else:
                    files.append(item)
            
            page_token = results.get('nextPageToken')
            if not page_token:
                break
        
        logger.info(f"📂 Listed {len(folders)} folders and {len(files)} files from folder {folder_id}")
        return folders, files, folder_id
    
    except HttpError as e:
        logger.error(f"HTTP Error listing files: {e}")
        if e.resp.status == 403:
            logger.error("Permission denied - check if service account has access to folder")
        return [], [], folder_id
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        return [], [], folder_id

def upload_to_drive(service, file_path, filename, parent_id):
    """Upload a file to Google Drive"""
    try:
        file_metadata = {
            'name': filename,
            'parents': [parent_id]
        }
        
        media = MediaFileUpload(
            file_path,
            resumable=True,
            chunksize=5*1024*1024  # 5MB chunks
        )
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, size, webViewLink',
            supportsAllDrives=True  # ADDED
        ).execute()
        
        return file
    
    except Exception as e:
        logger.error(f"Error uploading {filename}: {e}")
        raise

def download_from_drive(service, file_id, destination_path):
    """Download a file from Google Drive"""
    try:
        request = service.files().get_media(
            fileId=file_id,
            supportsAllDrives=True  # ADDED
        )
        
        fh = io.FileIO(destination_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request, chunksize=5*1024*1024)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                logger.debug(f"Download {progress}% complete")
        
        fh.close()
        return True
    
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return False

def search_drive(service, query_text, folder_id=None):
    """Search for files in Drive - FIXED VERSION"""
    try:
        # Escape special characters
        clean_query = query_text.replace("'", "\\'").replace('"', '\\"')
        
        # FIXED: Search in all subfolders
        if folder_id:
            query = f"name contains '{clean_query}' and '{folder_id}' in parents and trashed=false"
        else:
            # Search everything under main folder
            query = f"name contains '{clean_query}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false"
        
        results = service.files().list(
            q=query,
            fields="files(id, name, mimeType, size, modifiedTime, webViewLink, parents)",
            pageSize=50,
            spaces='drive',
            supportsAllDrives=True,  # CRITICAL FIX
            includeItemsFromAllDrives=True,  # CRITICAL FIX
            orderBy='modifiedTime desc'
        ).execute()
        
        return results.get('files', [])
    
    except Exception as e:
        logger.error(f"Error searching Drive: {e}")
        return []

# ==================== BROWSER HELPERS ====================
def get_breadcrumb(session):
    """Generate breadcrumb navigation"""
    if not session['path']:
        return "📁 Root"
    
    crumbs = ["Root"] + [p['name'] for p in session['path']]
    return " > ".join(crumbs)

def build_browser_keyboard(user_id, folders, files, total_items):
    """Build file browser keyboard with pagination"""
    session = BROWSER_SESSIONS.get(user_id, {'page': 0})
    page = session.get('page', 0)
    
    start_idx = page * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    
    # Combine folders and files
    all_items = folders + files
    page_items = all_items[start_idx:end_idx]
    
    keyboard = []
    
    # Add items
    for item in page_items:
        is_folder = item['mimeType'] == 'application/vnd.google-apps.folder'
        icon = "📁" if is_folder else "📄"
        name = item['name'][:30]  # Truncate long names
        
        callback_data = f"folder|{item['id']}" if is_folder else f"file|{item['id']}"
        keyboard.append([InlineKeyboardButton(f"{icon} {name}", callback_data=callback_data)])
    
    # Navigation row
    nav_row = []
    
    # Back button
    if session.get('path'):
        nav_row.append(InlineKeyboardButton("⬅️ Back", callback_data="browser_back"))
    
    # Pagination
    total_pages = (total_items - 1) // ITEMS_PER_PAGE + 1 if total_items > 0 else 1
    current_page = page + 1
    
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data="browser_prev"))
    
    if total_pages > 1:
        nav_row.append(InlineKeyboardButton(f"📄 {current_page}/{total_pages}", callback_data="browser_noop"))
    
    if end_idx < total_items:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data="browser_next"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    # Action row
    action_row = [
        InlineKeyboardButton("🔄 Refresh", callback_data="browser_refresh"),
        InlineKeyboardButton("🔍 Search", callback_data="browser_search"),
        InlineKeyboardButton("⭐ Favs", callback_data="browser_favorites")
    ]
    keyboard.append(action_row)
    
    # Close button
    keyboard.append([InlineKeyboardButton("❌ Close", callback_data="browser_close")])
    
    return InlineKeyboardMarkup(keyboard)

# ==================== QUEUE MANAGEMENT ====================
def add_to_queue(file_list, series_name=None, flat_upload=False):
    """Add upload task to queue"""
    global QUEUE_COUNTER
    QUEUE_COUNTER += 1
    queue_id = f"Q{QUEUE_COUNTER:04d}"
    
    UPLOAD_QUEUE[queue_id] = {
        'files': file_list,
        'series_name': series_name,
        'flat_upload': flat_upload,
        'status': 'queued',
        'created_at': time.time(),
        'progress': 0,
        'total': len(file_list)
    }
    
    logger.info(f"📋 Added to queue: {queue_id} ({len(file_list)} files)")
    return queue_id

def get_queue_status():
    """Get formatted queue status"""
    if not UPLOAD_QUEUE:
        return "📋 Queue is empty"
    
    status = "📋 **Upload Queue**\n\n"
    for queue_id, data in UPLOAD_QUEUE.items():
        progress = data.get('progress', 0)
        total = data.get('total', 0)
        state = data.get('status', 'unknown')
        name = data.get('series_name', 'N/A')
        
        status += f"`{queue_id}` - {name}\n"
        status += f"└ Status: {state} ({progress}/{total})\n\n"
    
    return status

# ==================== UPLOAD TASK ====================
async def upload_task(client, status_msg, file_list, series_name=None, flat_upload=False, queue_id=None):
    """Main upload task with progress tracking"""
    service = get_drive_service()
    if not service:
        await status_msg.edit_text("❌ Failed to connect to Google Drive")
        return
    
    # Create task ID for cancellation
    task_id = f"upload_{int(time.time())}"
    ACTIVE_TASKS[task_id] = {'cancelled': False}
    
    try:
        # Update queue status
        if queue_id and queue_id in UPLOAD_QUEUE:
            UPLOAD_QUEUE[queue_id]['status'] = 'uploading'
        
        # Determine upload strategy
        if flat_upload:
            # Upload directly to root
            parent_folder_id = DRIVE_FOLDER_ID
        elif series_name:
            # Create series folder
            parent_folder_id = get_or_create_folder(service, series_name, DRIVE_FOLDER_ID)
        else:
            # Each file in its own folder
            parent_folder_id = None
        
        total_files = len(file_list)
        uploaded = 0
        failed = []
        global TOTAL_FILES, TOTAL_BYTES
        
        for idx, file_info in enumerate(file_list, 1):
            # Check for cancellation
            if ACTIVE_TASKS[task_id].get('cancelled'):
                await status_msg.edit_text(f"🛑 **Upload Cancelled**\n\n{uploaded}/{total_files} files uploaded")
                break
            
            try:
                msg = await client.get_messages(file_info['chat_id'], file_info['message_id'])
                
                # Update progress
                await status_msg.edit_text(
                    f"📤 **Uploading** ({idx}/{total_files})\n"
                    f"📄 {file_info['name']}\n"
                    f"📊 {format_size(file_info['size'])}"
                )
                
                # Download from Telegram
                local_path = await msg.download(file_name=f"downloads/{file_info['name']}")
                
                # Determine parent folder
                if not flat_upload and not series_name:
                    # Create individual folder for this file
                    folder_name = os.path.splitext(file_info['name'])[0]
                    upload_parent = get_or_create_folder(service, folder_name, DRIVE_FOLDER_ID)
                else:
                    upload_parent = parent_folder_id
                
                # Upload to Drive
                result = upload_to_drive(service, local_path, file_info['name'], upload_parent)
                
                # Update stats
                TOTAL_FILES += 1
                TOTAL_BYTES += file_info['size']
                uploaded += 1
                
                # Update queue progress
                if queue_id and queue_id in UPLOAD_QUEUE:
                    UPLOAD_QUEUE[queue_id]['progress'] = uploaded
                
                # Clean up local file
                try:
                    os.remove(local_path)
                except:
                    pass
                
            except Exception as e:
                logger.error(f"Failed to upload {file_info['name']}: {e}")
                failed.append(file_info['name'])
        
        # Final summary
        summary = f"✅ **Upload Complete**\n\n"
        summary += f"📤 Uploaded: {uploaded}/{total_files}\n"
        
        if failed:
            summary += f"❌ Failed: {len(failed)}\n"
            summary += f"Files: {', '.join(failed[:5])}"
            if len(failed) > 5:
                summary += f"... and {len(failed)-5} more"
        
        if series_name:
            summary += f"\n📁 Series: {series_name}"
        
        summary += f"\n📊 Total: {format_size(TOTAL_BYTES)}"
        
        await status_msg.edit_text(summary)
        
        # Save stats
        sync_stats(service, save=True)
        
        # Remove from queue
        if queue_id and queue_id in UPLOAD_QUEUE:
            del UPLOAD_QUEUE[queue_id]
    
    except Exception as e:
        logger.error(f"Upload task error: {e}")
        await status_msg.edit_text(f"❌ Upload failed: {str(e)}")
    
    finally:
        # Clean up task
        if task_id in ACTIVE_TASKS:
            del ACTIVE_TASKS[task_id]

# ==================== HELPER FUNCTIONS ====================
def format_size(size_bytes):
    """Format bytes to human readable"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def clean_series_name(filename):
    """Extract series name from filename"""
    # Remove file extension
    name = os.path.splitext(filename)[0]
    
    # Remove common patterns
    patterns = [
        r'\[.*?\]',  # [tags]
        r'\(.*?\)',  # (info)
        r'\d+x\d+',  # Episode numbers
        r'[Ss]\d+[Ee]\d+',  # S01E01
        r'\d{3,4}p',  # Quality
        r'\.', # Dots
    ]
    
    for pattern in patterns:
        name = re.sub(pattern, ' ', name)
    
    # Clean up whitespace
    name = ' '.join(name.split())
    
    return name.strip()

# ==================== BOT COMMANDS ====================
@app.on_message(filters.command("start") & filters.user(OWNER_ID))
async def start_command(client, message: Message):
    """Start command"""
    await message.reply_text(
        "🤖 **RxUploader Bot**\n\n"
        "Send me files to upload to Google Drive!\n\n"
        "**Commands:**\n"
        "/browse - Browse Drive files\n"
        "/search <query> - Search files\n"
        "/stats - View statistics\n"
        "/queue - Check upload queue\n"
        "/favorites - Manage favorites"
    )

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_command(client, message: Message):
    """Show bot statistics"""
    uptime = time.time() - START_TIME
    hours = int(uptime // 3600)
    minutes = int((uptime % 3600) // 60)
    
    stats_text = (
        f"📊 **Bot Statistics**\n\n"
        f"⏱ Uptime: {hours}h {minutes}m\n"
        f"📁 Total Files: {TOTAL_FILES:,}\n"
        f"💾 Total Size: {format_size(TOTAL_BYTES)}\n"
        f"📋 Queue Size: {len(UPLOAD_QUEUE)}\n"
        f"🔄 Active Tasks: {len(ACTIVE_TASKS)}"
    )
    
    await message.reply_text(stats_text)

@app.on_message(filters.command("browse") & filters.user(OWNER_ID))
async def browse_command(client, message: Message):
    """Browse Google Drive files"""
    service = get_drive_service()
    if not service:
        await message.reply_text("❌ Failed to connect to Google Drive")
        return
    
    user_id = message.from_user.id
    
    # Initialize or reset session
    BROWSER_SESSIONS[user_id] = {
        'current_folder': DRIVE_FOLDER_ID,
        'path': [],
        'selected_files': [],
        'page': 0
    }
    
    # List files
    folders, files, _ = list_drive_files(service, DRIVE_FOLDER_ID)
    total_items = len(folders) + len(files)
    
    keyboard = build_browser_keyboard(user_id, folders, files, total_items)
    breadcrumb = get_breadcrumb(BROWSER_SESSIONS[user_id])
    
    await message.reply_text(
        f"📁 **File Browser**\n"
        f"📍 {breadcrumb}\n"
        f"📊 {len(folders)} folders, {len(files)} files",
        reply_markup=keyboard
    )

@app.on_message(filters.command("search") & filters.user(OWNER_ID))
async def search_command(client, message: Message):
    """Search for files in Drive"""
    # Extract search query
    query_text = message.text.replace("/search", "").strip()
    
    if not query_text:
        await message.reply_text("❓ Usage: /search <query>")
        return
    
    service = get_drive_service()
    if not service:
        await message.reply_text("❌ Failed to connect to Google Drive")
        return
    
    # Search
    status = await message.reply_text(f"🔍 Searching for: `{query_text}`...")
    results = search_drive(service, query_text)
    
    if not results:
        await status.edit_text(f"❌ No results found for: `{query_text}`")
        return
    
    # Format results
    response = f"🔍 **Search Results** ({len(results)})\n\n"
    
    for item in results[:20]:  # Limit to 20 results
        icon = "📁" if item['mimeType'] == 'application/vnd.google-apps.folder' else "📄"
        name = item['name']
        size = format_size(int(item.get('size', 0))) if 'size' in item else "N/A"
        response += f"{icon} `{name}`\n└ Size: {size}\n\n"
    
    if len(results) > 20:
        response += f"... and {len(results)-20} more results"
    
    await status.edit_text(response)

@app.on_message(filters.command("queue") & filters.user(OWNER_ID))
async def queue_command(client, message: Message):
    """Check upload queue"""
    status = get_queue_status()
    await message.reply_text(status)

@app.on_message(filters.command("favorites") & filters.user(OWNER_ID))
async def favorites_command(client, message: Message):
    """Show favorites"""
    user_id = message.from_user.id
    
    if user_id not in FAVORITES or not FAVORITES[user_id]:
        await message.reply_text("⭐ No favorites yet!")
        return
    
    favs = FAVORITES[user_id]
    response = f"⭐ **Your Favorites** ({len(favs)})\n\n"
    
    for fav in favs:
        response += f"📁 {fav['name']}\n"
    
    # Add clear button
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑️ Clear All", callback_data="browser_clear_favs")]
    ])
    
    await message.reply_text(response, reply_markup=keyboard)

# ==================== FILE HANDLERS ====================
@app.on_message((filters.document | filters.video | filters.audio) & filters.user(OWNER_ID))
async def handle_media(client, message: Message):
    """Handle incoming media files"""
    global ALBUMS
    
    # Get file info
    media = message.document or message.video or message.audio
    file_name = getattr(media, 'file_name', f"file_{int(time.time())}")
    file_size = media.file_size
    caption = message.caption
    
    file_info = {
        'name': file_name,
        'size': file_size,
        'caption': caption,
        'message_id': message.id,
        'chat_id': message.chat.id
    }
    
    # Check if part of an album
    if message.media_group_id:
        group_id = message.media_group_id
        
        if group_id not in ALBUMS:
            ALBUMS[group_id] = {
                'files': [],
                'timer': None,
                'first_msg': message
            }
        
        ALBUMS[group_id]['files'].append(file_info)
        
        # Cancel existing timer
        if ALBUMS[group_id]['timer']:
            ALBUMS[group_id]['timer'].cancel()
        
        # Set new timer (2 seconds to collect all files in album)
        async def process_album():
            await asyncio.sleep(2)
            if group_id in ALBUMS:
                await show_upload_options(client, ALBUMS[group_id]['first_msg'], ALBUMS[group_id]['files'])
                del ALBUMS[group_id]
        
        ALBUMS[group_id]['timer'] = asyncio.create_task(process_album())
    
    else:
        # Single file
        await show_upload_options(client, message, [file_info])

async def show_upload_options(client, message: Message, file_list):
    """Show upload mode selection"""
    key = f"upload_{int(time.time())}_{message.from_user.id}"
    TEMP_FILES[key] = file_list
    
    # Detect series name from first file
    first_file = file_list[0]['name']
    auto_series = clean_series_name(first_file)
    
    # Check for caption
    has_caption = any(f.get('caption') for f in file_list)
    caption_name = next((f['caption'] for f in file_list if f.get('caption')), None)
    
    # Build keyboard
    keyboard = [
        [InlineKeyboardButton("📁 Standalone (Each file = 1 folder)", callback_data=f"std|{key}")],
        [InlineKeyboardButton("📂 Root (No folders)", callback_data=f"root|{key}")]
    ]
    
    if len(file_list) > 1:
        if has_caption:
            keyboard.append([InlineKeyboardButton(f"📚 Series: {caption_name}", callback_data=f"cap|{key}")])
        
        keyboard.append([InlineKeyboardButton(f"🤖 Auto: {auto_series}", callback_data=f"auto|{key}")])
        keyboard.append([InlineKeyboardButton("✏️ Custom Series Name", callback_data=f"custom|{key}")])
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_selection|{key}")])
    
    total_size = sum(f['size'] for f in file_list)
    
    await message.reply_text(
        f"📥 **Upload Mode Selection**\n\n"
        f"Files: {len(file_list)}\n"
        f"Size: {format_size(total_size)}\n\n"
        f"Choose upload mode:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ==================== CALLBACK HANDLERS ====================
@app.on_callback_query()
async def callback_handler(client, query):
    """Handle all callback queries"""
    try:
        user_id = query.from_user.id
        service = get_drive_service()
        
        # ==================== BROWSER CALLBACKS ====================
        if query.data.startswith("browser_") or query.data.startswith("folder|") or query.data.startswith("file|"):
            
            if not service:
                await query.answer("❌ Drive connection failed", show_alert=True)
                return
            
            session = BROWSER_SESSIONS.get(user_id)
            if not session and not query.data in ["browser_close", "browser_search", "browser_favorites", "browser_clear_favs"]:
                await query.answer("⚠️ Session expired. Use /browse to start again.", show_alert=True)
                return
            
            # Close browser
            if query.data == "browser_close":
                try:
                    await query.message.delete()
                except:
                    await query.message.edit_text("✅ Browser closed")
                await query.answer()
                return
            
            # Navigate to folder
            if query.data.startswith("folder|"):
                folder_id = query.data.replace("folder|", "")
                
                # Get folder info
                try:
                    folder = service.files().get(
                        fileId=folder_id,
                        fields="id, name",
                        supportsAllDrives=True
                    ).execute()
                    
                    # Update session
                    session['path'].append({'id': session['current_folder'], 'name': folder['name']})
                    session['current_folder'] = folder_id
                    session['page'] = 0
                    
                    # List new folder contents
                    folders, files, _ = list_drive_files(service, folder_id)
                    total_items = len(folders) + len(files)
                    keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                    breadcrumb = get_breadcrumb(session)
                    
                    # FIXED: Check if content actually changed before editing
                    new_text = (
                        f"📁 **File Browser**\n"
                        f"📍 {breadcrumb}\n"
                        f"📊 {len(folders)} folders, {len(files)} files"
                    )
                    
                    try:
                        await query.message.edit_text(new_text, reply_markup=keyboard)
                    except MessageNotModified:
                        # Content is the same, just answer the callback
                        pass
                    
                    await query.answer()
                    
                except Exception as e:
                    await query.answer(f"❌ Error: {str(e)}", show_alert=True)
                
                return
            
            # File selection
            if query.data.startswith("file|"):
                file_id = query.data.replace("file|", "")
                await query.answer("📄 File options coming soon!", show_alert=True)
                return
            
            # Back navigation
            if query.data == "browser_back":
                if session['path']:
                    prev = session['path'].pop()
                    session['current_folder'] = prev['id']
                    session['page'] = 0
                    
                    folders, files, _ = list_drive_files(service, session['current_folder'])
                    total_items = len(folders) + len(files)
                    keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                    breadcrumb = get_breadcrumb(session)
                    
                    new_text = (
                        f"📁 **File Browser**\n"
                        f"📍 {breadcrumb}\n"
                        f"📊 {len(folders)} folders, {len(files)} files"
                    )
                    
                    try:
                        await query.message.edit_text(new_text, reply_markup=keyboard)
                    except MessageNotModified:
                        pass
                    
                    await query.answer()
                else:
                    await query.answer("Already at root", show_alert=False)
                
                return
            
            # Pagination
            if query.data == "browser_prev":
                session['page'] = max(0, session['page'] - 1)
                
                folders, files, _ = list_drive_files(service, session['current_folder'])
                total_items = len(folders) + len(files)
                keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                breadcrumb = get_breadcrumb(session)
                
                new_text = (
                    f"📁 **File Browser**\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(folders)} folders, {len(files)} files"
                )
                
                try:
                    await query.message.edit_text(new_text, reply_markup=keyboard)
                except MessageNotModified:
                    pass
                
                await query.answer()
                return
            
            if query.data == "browser_next":
                folders, files, _ = list_drive_files(service, session['current_folder'])
                total_items = len(folders) + len(files)
                max_page = (total_items - 1) // ITEMS_PER_PAGE
                
                session['page'] = min(max_page, session['page'] + 1)
                
                keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                breadcrumb = get_breadcrumb(session)
                
                new_text = (
                    f"📁 **File Browser**\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(folders)} folders, {len(files)} files"
                )
                
                try:
                    await query.message.edit_text(new_text, reply_markup=keyboard)
                except MessageNotModified:
                    pass
                
                await query.answer()
                return
            
            # No-op for page indicator
            if query.data == "browser_noop":
                await query.answer()
                return
            
            # Refresh
            if query.data == "browser_refresh":
                folders, files, _ = list_drive_files(service, session['current_folder'])
                total_items = len(folders) + len(files)
                keyboard = build_browser_keyboard(user_id, folders, files, total_items)
                breadcrumb = get_breadcrumb(session)
                
                new_text = (
                    f"📁 **File Browser**\n"
                    f"📍 {breadcrumb}\n"
                    f"📊 {len(folders)} folders, {len(files)} files"
                )
                
                try:
                    await query.message.edit_text(new_text, reply_markup=keyboard)
                except MessageNotModified:
                    pass
                
                await query.answer("🔄 Refreshed", show_alert=False)
                return
            
            # Search button
            if query.data == "browser_search":
                await query.answer("🔍 Use /search <query> command", show_alert=True)
                return
            
            # Favorites button
            if query.data == "browser_favorites":
                await query.answer("⭐ Use /favorites command", show_alert=True)
                return
            
            # Clear favorites
            if query.data == "browser_clear_favs":
                if user_id in FAVORITES:
                    count = len(FAVORITES[user_id])
                    FAVORITES[user_id] = []
                    await query.answer(f"🗑️ Cleared {count} favorites", show_alert=True)
                    await query.message.edit_text("⭐ **Favorites cleared!**")
                else:
                    await query.answer("ℹ️ No favorites to clear", show_alert=True)
                return
        
        # ==================== UPLOAD CALLBACKS ====================
        # Cancel selection
        if query.data.startswith("cancel_selection|"):
            key = query.data.replace("cancel_selection|", "")
            
            if key in TEMP_FILES:
                file_count = len(TEMP_FILES[key])
                del TEMP_FILES[key]
                
                await query.answer("✅ Cancelled", show_alert=False)
                await query.message.edit_text(
                    f"❌ **Upload Cancelled**\n\n"
                    f"{file_count} file(s) discarded."
                )
            else:
                await query.answer("ℹ️ Already processed", show_alert=True)
            return
        
        # Cancel upload
        if query.data.startswith("cancel_"):
            task_id = query.data.replace("cancel_", "")
            
            if task_id in ACTIVE_TASKS:
                ACTIVE_TASKS[task_id]['cancelled'] = True
                
                await query.answer("🛑 Cancelling...", show_alert=True)
                await query.message.edit_text("🛑 **Cancellation Requested**\n\nPlease wait...")
            else:
                await query.answer("ℹ️ Task already completed", show_alert=True)
            return
        
        # Upload mode selection
        data_parts = query.data.split('|')
        mode = data_parts[0]
        key = data_parts[1] if len(data_parts) > 1 else None
        
        if not key or key not in TEMP_FILES:
            await query.answer("❌ Session expired. Please send the file again.", show_alert=True)
            return
        
        file_list = TEMP_FILES[key]
        
        if mode == "std":
            queue_id = add_to_queue(file_list, series_name=None, flat_upload=False)
            await query.message.edit_text(f"🚀 Starting upload...\n📋 Queue ID: `{queue_id}`")
            asyncio.create_task(upload_task(client, query.message, file_list, series_name=None, queue_id=queue_id))
            del TEMP_FILES[key]
        
        elif mode == "root":
            queue_id = add_to_queue(file_list, series_name=None, flat_upload=True)
            await query.message.edit_text(f"🚀 Uploading to Root...\n📋 Queue ID: `{queue_id}`")
            asyncio.create_task(upload_task(client, query.message, file_list, series_name=None, flat_upload=True, queue_id=queue_id))
            del TEMP_FILES[key]
        
        elif mode == "cap":
            series_name = next((f['caption'] for f in file_list if f['caption']), "Unknown Series")
            queue_id = add_to_queue(file_list, series_name=series_name, flat_upload=False)
            await query.message.edit_text(f"🚀 Series: **{series_name}**\n📋 Queue ID: `{queue_id}`")
            asyncio.create_task(upload_task(client, query.message, file_list, series_name=series_name, queue_id=queue_id))
            del TEMP_FILES[key]
        
        elif mode == "auto":
            first_file = file_list[0]['name']
            series_name = clean_series_name(first_file)
            queue_id = add_to_queue(file_list, series_name=series_name, flat_upload=False)
            await query.message.edit_text(f"🚀 Series: **{series_name}**\n📋 Queue ID: `{queue_id}`")
            asyncio.create_task(upload_task(client, query.message, file_list, series_name=series_name, queue_id=queue_id))
            del TEMP_FILES[key]
        
        elif mode == "custom":
            ACTIVE_SERIES[user_id] = {
                'file_list': file_list,
                'key': key
            }
            await query.message.edit_text(
                "✏️ **Enter Custom Series Name**\n\n"
                "Reply with the series name you want to use."
            )
        
        await query.answer()
    
    except MessageNotModified:
        # Silently ignore - message content is the same
        await query.answer()
    except Exception as e:
        logger.error(f"Callback error: {e}")
        await query.answer(f"Error: {str(e)}", show_alert=True)

@app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "stats", "browse", "search", "queue", "favorites"]))
async def handle_text(client, message: Message):
    """Handle text messages (custom series names)"""
    user_id = message.from_user.id
    
    if user_id in ACTIVE_SERIES:
        series_data = ACTIVE_SERIES[user_id]
        file_list = series_data['file_list']
        key = series_data['key']
        series_name = message.text.strip()
        
        # Clean up
        del ACTIVE_SERIES[user_id]
        if key in TEMP_FILES:
            del TEMP_FILES[key]
        
        queue_id = add_to_queue(file_list, series_name=series_name, flat_upload=False)
        status = await message.reply_text(f"🚀 Series: **{series_name}**\n📋 Queue ID: `{queue_id}`")
        asyncio.create_task(upload_task(client, status, file_list, series_name=series_name, queue_id=queue_id))

# ==================== MAIN ====================
if __name__ == "__main__":
    logger.info("🤖 Starting RxUploader...")
    
    # Create downloads directory
    os.makedirs("downloads", exist_ok=True)
    
    # Load stats
    try:
        service = get_drive_service()
        if service:
            sync_stats(service, save=False)
    except Exception as e:
        logger.error(f"Failed to load stats: {e}")
    
    logger.info("✅ Bot initialized!")
    logger.info(f"👤 Owner ID: {OWNER_ID}")
    logger.info(f"📁 Drive Folder: {DRIVE_FOLDER_ID}")
    
    app.run()
