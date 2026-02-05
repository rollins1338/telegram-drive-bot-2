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
from pyrogram.errors import FloodWait
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
# Ultra-minimal health check (Koyeb-tested and working)
def run_health_server():
    try:
        HTTPServer(('0.0.0.0', 8000), type('H', (BaseHTTPRequestHandler,), {
            'do_GET': lambda s: (s.send_response(200), s.end_headers(), s.wfile.write(b"OK"))
        })).serve_forever()
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
    """Find existing folder or create new one"""
    try:
        # Sanitize folder name
        clean_name = folder_name.replace('"', '\\"')
        
        # Search for existing folder
        query = f'name="{clean_name}" and "{parent_id}" in parents and mimeType="application/vnd.google-apps.folder" and trashed=false'
        results = service.files().list(
            q=query,
            fields="files(id, name)"
        ).execute()
        
        folders = results.get('files', [])
        
        if folders:
            return folders[0]['id']
        
        # Create new folder
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        
        folder = service.files().create(
            body=file_metadata,
            fields='id'
        ).execute()
        
        logger.info(f"📁 Created new folder: {folder_name}")
        return folder.get('id')
    
    except HttpError as e:
        logger.error(f"HTTP error creating folder: {e}")
        return None
    except Exception as e:
        logger.error(f"Error creating folder: {e}")
        return None

def clean_series_name(filename):
    """
    Extract clean series name from filename
    Removes quality tags, group names, episode info, etc.
    """
    # Remove file extension
    name = os.path.splitext(filename)[0]
    
    # Remove group tags [Group] or (Group)
    name = re.sub(r'[\[\(][^\]\)]*[\]\)]', '', name)
    
    # Remove quality indicators
    name = re.sub(r'\b(1080p|720p|480p|2160p|4K|UHD|HDR|BluRay|BRRip|WEBRip|WEBDL|WEB-DL|DVDRip|HDTV|x264|x265|HEVC|AAC|AC3|DTS|DD5\.1|10bit|8bit)\b', '', name, flags=re.IGNORECASE)
    
    # Remove episode patterns
    name = re.sub(r'\b(S\d+E\d+|Season\s*\d+|Episode\s*\d+|\d+x\d+|E\d+|Ep\d+)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*-\s*\d+\s*$', '', name)  # Remove trailing - 01, - 02, etc.
    
    # Clean up separators
    name = name.replace('.', ' ').replace('_', ' ').replace('-', ' ')
    
    # Remove extra spaces
    name = ' '.join(name.split()).strip()
    
    return name or "Unknown Series"

def upload_to_drive_with_progress(service, file_path, file_metadata, message_data, filename):
    """
    Upload file to Google Drive with progress tracking
    message_data is a dict with {'chat_id': int, 'message_id': int, 'bot': Client}
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
async def progress_callback(current, total, message, start_time, filename):
    """Show upload/download progress with accurate speed"""
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
            await message.edit_text(
                f"📊 **Progress**\n"
                f"📄 `{filename[:40]}...`\n\n"
                f"[{bar}] {percentage:.1f}%\n"
                f"⚡ Speed: {speed/1024/1024:.2f} MB/s\n"
                f"💾 {current/1024/1024:.1f} MB / {total/1024/1024:.1f} MB\n"
                f"⏱️ ETA: {eta}"
            )
        except Exception:
            pass
        
        # Update tracking variables
        message.last_update = now
        message.last_current = current
        message.last_time = now

# ==================== UPLOAD TASK ====================
async def upload_task(client: Client, status_msg: Message, file_list: list, series_name: str = None, flat_upload: bool = False):
    """
    Main upload task - handles batch uploading with proper organization
    
    Args:
        client: Pyrogram client
        status_msg: Status message to update
        file_list: List of dicts with 'msg_id' and 'name'
        series_name: Optional series name for organization
        flat_upload: If True, upload directly to parent folder without subfolders
    """
    global TOTAL_FILES, TOTAL_BYTES
    
    service = get_drive_service()
    if not service:
        await status_msg.edit_text("❌ **Failed to connect to Google Drive!**\n\nPlease check TOKEN_JSON configuration.")
        return
    
    try:
        await status_msg.edit_text(f"🔄 **Preparing folders...**\n📦 Files to upload: {len(file_list)}")
        
        # Determine parent folder
        if series_name and not flat_upload:
            # Create series folder
            parent_folder = get_or_create_folder(service, series_name, DRIVE_FOLDER_ID)
            if not parent_folder:
                await status_msg.edit_text(f"❌ **Failed to create series folder:** {series_name}")
                return
        else:
            parent_folder = DRIVE_FOLDER_ID
        
        # Create downloads directory if not exists
        os.makedirs("downloads", exist_ok=True)
        
        successful_uploads = 0
        failed_uploads = []
        
        # Process each file
        for idx, file_info in enumerate(file_list, 1):
            filename = file_info['name']
            msg_id = file_info['msg_id']
            download_path = None
            
            try:
                # Update status
                await status_msg.edit_text(
                    f"📥 **Downloading ({idx}/{len(file_list)})**\n"
                    f"📄 `{filename}`"
                )
                
                # Download file
                download_path = f"downloads/{filename}"
                start_time = time.time()
                
                try:
                    # Get the message object
                    message = await client.get_messages(status_msg.chat.id, msg_id)
                    
                    await client.download_media(
                        message,
                        file_name=download_path,
                        progress=progress_callback,
                        progress_args=(status_msg, start_time, filename)
                    )
                except FloodWait as e:
                    logger.warning(f"FloodWait: Sleeping for {e.value} seconds")
                    await asyncio.sleep(e.value)
                    # Retry download
                    message = await client.get_messages(status_msg.chat.id, msg_id)
                    await client.download_media(
                        message,
                        file_name=download_path,
                        progress=progress_callback,
                        progress_args=(status_msg, start_time, filename)
                    )
                
                if not os.path.exists(download_path):
                    raise Exception("Download failed - file not found")
                
                file_size = os.path.getsize(download_path)
                
                # Handle Folder Logic
                if flat_upload:
                     # Not an Audiobook: Upload directly to Root/Parent
                    upload_folder = parent_folder
                else:
                    # Standard behavior: Create individual folder for the file
                    folder_name = os.path.splitext(filename)[0]
                    file_folder = get_or_create_folder(service, folder_name, parent_folder)
                    
                    if not file_folder:
                        raise Exception("Failed to create file folder")
                    
                    upload_folder = file_folder
                
                # Upload to Drive with progress
                file_metadata = {
                    'name': filename,
                    'parents': [upload_folder]
                }
                
                # Initial upload message
                await status_msg.edit_text(
                    f"☁️ **Uploading to Drive ({idx}/{len(file_list)})**\n"
                    f"📄 `{filename}`\n"
                    f"💾 Size: {file_size/1024/1024:.2f} MB\n\n"
                    f"Starting upload..."
                )
                
                # Shared data for progress tracking
                progress_data = {'complete': False, 'error': None, 'last_progress': None}
                
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
                
                # Monitor progress while upload is running
                while not progress_data.get('complete') and not progress_data.get('error'):
                    if progress_data.get('last_progress'):
                        p = progress_data['last_progress']
                        try:
                            await status_msg.edit_text(
                                f"☁️ **Uploading to Drive ({idx}/{len(file_list)})**\n"
                                f"📄 `{p['filename'][:40]}...`\n\n"
                                f"[{p['bar']}] {p['progress']}%\n"
                                f"⚡ Speed: {p['speed']/1024/1024:.2f} MB/s\n"
                                f"💾 {p['current']/1024/1024:.1f} MB / {p['total']/1024/1024:.1f} MB\n"
                                f"⏱️ ETA: {p['eta']}"
                            )
                        except:
                            pass
                    
                    # Check every second
                    await asyncio.sleep(1)
                
                # Wait for upload to complete
                upload_result = await upload_future
                
                if progress_data.get('error'):
                    raise Exception(progress_data['error'])
                
                # Update stats
                uploaded_size = int(upload_result.get('size', file_size))
                TOTAL_FILES += 1
                TOTAL_BYTES += uploaded_size
                
                # Clean up downloaded file
                if os.path.exists(download_path):
                    os.remove(download_path)
                
                successful_uploads += 1
                logger.info(f"✅ Uploaded: {filename} ({uploaded_size/1024/1024:.2f} MB)")
            
            except Exception as e:
                logger.error(f"❌ Failed to upload {filename}: {e}")
                failed_uploads.append(f"{filename}: {str(e)[:50]}")
                
                # Clean up on error
                if download_path and os.path.exists(download_path):
                    try:
                        os.remove(download_path)
                    except:
                        pass
        
        # Save stats
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, sync_stats, service, True)
        except Exception as e:
            logger.error(f"Failed to save stats: {e}")
        
        # Final status
        location_text = "Root (No Folders)" if flat_upload else (f"**{series_name}**" if series_name else "Root -> Standalone")
        status_text = f"✅ **Upload Complete!**\n\n"
        status_text += f"📁 Location: {location_text}\n"
        status_text += f"✅ Successful: {successful_uploads}/{len(file_list)}\n"
        
        if failed_uploads:
            status_text += f"❌ Failed: {len(failed_uploads)}\n\n**Errors:**\n"
            for error in failed_uploads[:5]:  # Show first 5 errors
                status_text += f"• {error}\n"
        
        status_text += f"\n📊 Total: {TOTAL_FILES} files | {TOTAL_BYTES/1024/1024/1024:.2f} GB"
        
        await status_msg.edit_text(status_text)
    
    except Exception as e:
        logger.error(f"Upload task error: {e}")
        await status_msg.edit_text(f"❌ **Error:** {str(e)}")

# ==================== BOT HANDLERS ====================
@app.on_message(filters.command("start") & filters.user(OWNER_ID))
async def start_command(client, message):
    """Start command"""
    await message.reply_text(
        "👋 **Hi, I'm RxUploader**\n\n"
        "📤 Send me files and I'll upload them to Rx's Google Drive!\n\n"
        "**Modes:**\n"
        "• 📂 Series: (Auto or Caption) -> Series Folder -> Book Folder\n"
        "• 📁 Standalone: Root -> Book Folder\n"
        "• 🚫 Not an Audiobook: Root (No Folders)\n\n"
        "**Commands:**\n"
        "/start - Show this message\n"
        "/stats - View upload statistics\n\n"
        "Just send files or albums to get started!"
    )

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_command(client, message):
    """Show upload statistics"""
    uptime = str(datetime.timedelta(seconds=int(time.time() - START_TIME)))
    
    stats_text = (
        f"📊 **Bot Statistics**\n\n"
        f"⏱️ **Uptime:** `{uptime}`\n"
        f"📁 **Total Files:** `{TOTAL_FILES:,}`\n"
        f"💾 **Total Data:** `{TOTAL_BYTES/1024/1024/1024:.2f} GB`\n"
    )
    
    if TOTAL_FILES > 0:
        stats_text += f"📈 **Average File Size:** `{(TOTAL_BYTES/TOTAL_FILES/1024/1024):.2f} MB`"
    
    await message.reply_text(stats_text)

@app.on_message(filters.media & filters.user(OWNER_ID))
async def handle_media(client, message: Message):
    """Handle incoming media files"""
    try:
        # Get file object and name
        media_type = message.media.value
        file_obj = getattr(message, media_type)
        filename = getattr(file_obj, 'file_name', None)
        caption = message.caption or "" # Capture caption
        
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
            # 3. Not an Audiobook
            buttons.append([InlineKeyboardButton("✏️ Custom Series Name", callback_data=f"custom|{key}")])
            # 4. Custom
            buttons.append([InlineKeyboardButton("🚫 Not an Audiobook", callback_data=f"root|{key}")])
            
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
        data_parts = query.data.split('|')
        mode = data_parts[0]
        key = data_parts[1] if len(data_parts) > 1 else None
        
        if not key or key not in TEMP_FILES:
            await query.answer("❌ Session expired. Please send the file again.", show_alert=True)
            return
        
        file_list = TEMP_FILES[key]
        
        if mode == "std":
            # Standalone mode - each file in its own folder
            await query.message.edit_text(f"🚀 Starting upload...")
            asyncio.create_task(upload_task(client, query.message, file_list, series_name=None))
            del TEMP_FILES[key]
        
        elif mode == "root":
             # Not an Audiobook - Upload directly to Root (No folders)
            await query.message.edit_text(f"🚀 Uploading directly to Root...")
            asyncio.create_task(upload_task(client, query.message, file_list, series_name=None, flat_upload=True))
            del TEMP_FILES[key]

        elif mode == "cap":
             # Use detected caption
            series_name = next((f['caption'] for f in file_list if f['caption']), "Unknown Series")
            await query.message.edit_text(f"🚀 Series Upload: **{series_name}**")
            asyncio.create_task(upload_task(client, query.message, file_list, series_name=series_name))
            del TEMP_FILES[key]
        
        elif mode == "auto":
            # Auto-detected series name from filename
            first_file = file_list[0]['name']
            series_name = clean_series_name(first_file)
            await query.message.edit_text(f"🚀 Series Upload: **{series_name}**")
            asyncio.create_task(upload_task(client, query.message, file_list, series_name=series_name))
            del TEMP_FILES[key]
        
        elif mode == "custom":
            # Ask for custom series name
            ACTIVE_SERIES[query.from_user.id] = {
                'file_list': file_list,
                'key': key
            }
            await query.message.edit_text(
                " ` Enter Custom Series Name `\n\n"
                
            )
        
        await query.answer()
    
    except Exception as e:
        logger.error(f"Callback error: {e}")
        await query.answer(f"Error: {str(e)}", show_alert=True)

@app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "stats"]))
async def handle_text(client, message: Message):
    """Handle text messages (series names)"""
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
        
        status = await message.reply_text(f"🚀 Starting series upload: **{series_name}**")
        asyncio.create_task(upload_task(client, status, file_list, series_name=series_name))

# ==================== MAIN ====================
if __name__ == "__main__":
    logger.info("🤖 Starting Telegram to Google Drive Bot...")
    
    # Create downloads directory
    os.makedirs("downloads", exist_ok=True)
    
    # Load stats from Drive
    try:
        service = get_drive_service()
        if service:
            sync_stats(service, save=False)
    except Exception as e:
        logger.error(f"Failed to load initial stats: {e}")
    
    logger.info("✅ Bot initialized successfully!")
    logger.info(f"👤 Owner ID: {OWNER_ID}")
    logger.info(f"📁 Drive Folder: {DRIVE_FOLDER_ID}")
    
    # Run bot
    app.run()
