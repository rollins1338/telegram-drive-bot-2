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
        return service
    except Exception as e:
        logger.error(f"Error creating Drive service: {e}")
        return None

def sync_stats(service, save=False):
    """Sync upload statistics with Google Drive"""
    global TOTAL_FILES, TOTAL_BYTES
    if not service: return
    stats_filename = "bot_stats.json"
    try:
        query = f"name='{stats_filename}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        file_id = files[0]['id'] if files else None
        
        if save:
            stats_data = json.dumps({
                "total_files": TOTAL_FILES,
                "total_bytes": TOTAL_BYTES,
                "last_updated": datetime.datetime.now().isoformat()
            })
            media = MediaIoBaseUpload(io.BytesIO(stats_data.encode()), mimetype='application/json')
            if file_id: service.files().update(fileId=file_id, media_body=media).execute()
            else: service.files().create(body={'name': stats_filename, 'parents': [DRIVE_FOLDER_ID]}, media_body=media).execute()
        else:
            if file_id:
                request = service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done: _, done = downloader.next_chunk()
                stats = json.loads(fh.getvalue().decode())
                TOTAL_FILES = stats.get('total_files', 0)
                TOTAL_BYTES = stats.get('total_bytes', 0)
    except Exception as e:
        logger.error(f"Error syncing stats: {e}")

def get_or_create_folder(service, folder_name, parent_id):
    """Find existing folder or create new one"""
    try:
        clean_name = folder_name.replace('"', '\\"')
        query = f'name="{clean_name}" and "{parent_id}" in parents and mimeType="application/vnd.google-apps.folder" and trashed=false'
        results = service.files().list(q=query, fields="files(id, name)").execute()
        folders = results.get('files', [])
        
        if folders: return folders[0]['id']
        
        file_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
        folder = service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')
    except Exception as e:
        logger.error(f"Error creating folder: {e}")
        return None

def clean_series_name(filename):
    """Extract clean series name from filename"""
    name = os.path.splitext(filename)[0]
    name = re.sub(r'[\[\(][^\]\)]*[\]\)]', '', name)
    name = re.sub(r'\b(1080p|720p|480p|2160p|4K|UHD|HDR|BluRay|BRRip|WEBRip|WEBDL|WEB-DL|DVDRip|HDTV|x264|x265|HEVC|AAC|AC3|DTS|DD5\.1|10bit|8bit)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(S\d+E\d+|Season\s*\d+|Episode\s*\d+|\d+x\d+|E\d+|Ep\d+)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*-\s*\d+\s*$', '', name)
    name = name.replace('.', ' ').replace('_', ' ').replace('-', ' ')
    name = ' '.join(name.split()).strip()
    return name or "Unknown Series"

def upload_to_drive_with_progress(service, file_path, file_metadata, message_data, filename):
    """Upload file to Google Drive with progress tracking"""
    try:
        media = MediaFileUpload(file_path, resumable=True, chunksize=5*1024*1024)
        request = service.files().create(body=file_metadata, media_body=media, fields='id, size')
        response = None
        last_progress = 0
        last_update = time.time()
        file_size = os.path.getsize(file_path)
        last_uploaded = 0
        
        while response is None:
            status, response = request.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                current_bytes = int(status.progress() * file_size)
                now = time.time()
                
                if (now - last_update) > 2 or progress - last_progress >= 5:
                    time_diff = now - last_update if (now - last_update) > 0 else 0.1
                    bytes_diff = current_bytes - last_uploaded
                    speed = bytes_diff / time_diff if time_diff > 0 else 0
                    
                    filled = int(progress // 10)
                    bar = '■' * filled + '□' * (10 - filled)
                    eta = str(datetime.timedelta(seconds=int((file_size - current_bytes) / speed))) if speed > 0 else "..."
                    
                    message_data['last_progress'] = {
                        'bar': bar, 'progress': progress, 'speed': speed,
                        'current': current_bytes, 'total': file_size, 'eta': eta, 'filename': filename
                    }
                    last_progress, last_update, last_uploaded = progress, now, current_bytes
        
        message_data['complete'] = True
        return response
    except Exception as e:
        message_data['error'] = str(e)
        raise

# ==================== PROGRESS CALLBACK ====================
async def progress_callback(current, total, message, start_time, filename):
    """Show upload/download progress with accurate speed"""
    now = time.time()
    if not hasattr(message, 'last_update'):
        message.last_update = 0
        message.last_current = 0
        message.last_time = start_time
    
    if (now - message.last_update) > 2 or current == total:
        time_diff = now - message.last_time
        speed = (current - message.last_current) / time_diff if time_diff > 0 else 0
        percentage = (current / total) * 100
        filled = int(percentage // 10)
        bar = '■' * filled + '□' * (10 - filled)
        
        try:
            await message.edit_text(
                f"📊 **Progress**\n📄 `{filename[:40]}...`\n\n[{bar}] {percentage:.1f}%\n"
                f"⚡ Speed: {speed/1024/1024:.2f} MB/s"
            )
        except: pass
        message.last_update, message.last_current, message.last_time = now, current, now

# ==================== UPLOAD TASK ====================
async def upload_task(client: Client, status_msg: Message, file_list: list, series_name: str = None, flat_upload: bool = False):
    """
    Main upload task - handles batch uploading with proper organization.
    flat_upload: If True, uploads directly to root/series folder without sub-folders.
    """
    global TOTAL_FILES, TOTAL_BYTES
    service = get_drive_service()
    if not service:
        await status_msg.edit_text("❌ **Failed to connect to Google Drive!**")
        return
    
    try:
        await status_msg.edit_text(f"🔄 **Preparing folders...**\n📦 Files to upload: {len(file_list)}")
        
        # Determine parent folder
        if series_name and not flat_upload:
            parent_folder = get_or_create_folder(service, series_name, DRIVE_FOLDER_ID)
        else:
            parent_folder = DRIVE_FOLDER_ID
        
        os.makedirs("downloads", exist_ok=True)
        successful_uploads = 0
        failed_uploads = []
        
        for idx, file_info in enumerate(file_list, 1):
            filename = file_info['name']
            msg_id = file_info['msg_id']
            download_path = f"downloads/{filename}"
            
            try:
                await status_msg.edit_text(f"📥 **Downloading ({idx}/{len(file_list)})**\n📄 `{filename}`")
                start_time = time.time()
                message = await client.get_messages(status_msg.chat.id, msg_id)
                await client.download_media(message, file_name=download_path, progress=progress_callback, progress_args=(status_msg, start_time, filename))
                
                file_size = os.path.getsize(download_path)
                
                # FOLDER LOGIC MODIFICATION:
                if flat_upload:
                    # Not an Audiobook: Upload directly to parent (Root)
                    upload_folder = parent_folder
                else:
                    # Standard behavior: Create folder for the book file
                    folder_name = os.path.splitext(filename)[0]
                    file_folder = get_or_create_folder(service, folder_name, parent_folder)
                    upload_folder = file_folder if file_folder else parent_folder

                file_metadata = {'name': filename, 'parents': [upload_folder]}
                
                # Initial upload message
                await status_msg.edit_text(f"☁️ **Uploading to Drive ({idx}/{len(file_list)})**\n📄 `{filename}`\nStarting upload...")
                
                progress_data = {'complete': False, 'error': None, 'last_progress': None}
                loop = asyncio.get_running_loop()
                upload_future = loop.run_in_executor(None, upload_to_drive_with_progress, service, download_path, file_metadata, progress_data, filename)
                
                while not progress_data.get('complete') and not progress_data.get('error'):
                    if progress_data.get('last_progress'):
                        p = progress_data['last_progress']
                        try: await status_msg.edit_text(f"☁️ **Uploading ({idx}/{len(file_list)})**\n📄 `{p['filename'][:40]}...`\n\n[{p['bar']}] {p['progress']}%\n⚡ {p['speed']/1024/1024:.2f} MB/s\n⏱️ ETA: {p['eta']}")
                        except: pass
                    await asyncio.sleep(1)
                
                upload_result = await upload_future
                if progress_data.get('error'): raise Exception(progress_data['error'])
                
                TOTAL_FILES += 1
                TOTAL_BYTES += int(upload_result.get('size', file_size))
                if os.path.exists(download_path): os.remove(download_path)
                successful_uploads += 1
                logger.info(f"✅ Uploaded: {filename}")
            
            except Exception as e:
                logger.error(f"❌ Failed {filename}: {e}")
                failed_uploads.append(f"{filename}: {str(e)[:50]}")
                if os.path.exists(download_path): os.remove(download_path)
        
        await loop.run_in_executor(None, sync_stats, service, True)
        
        status_text = f"✅ **Upload Complete!**\n\n"
        status_text += f"📁 Location: {'Root (No Folders)' if flat_upload else (series_name or 'Root -> Standalone')}\n"
        status_text += f"✅ Successful: {successful_uploads}/{len(file_list)}\n"
        
        if failed_uploads:
            status_text += f"\n❌ Failed: {len(failed_uploads)}"
        
        await status_msg.edit_text(status_text)
    
    except Exception as e:
        logger.error(f"Task error: {e}")
        await status_msg.edit_text(f"❌ **Error:** {str(e)}")

# ==================== BOT HANDLERS ====================
@app.on_message(filters.command("start") & filters.user(OWNER_ID))
async def start_command(client, message):
    await message.reply_text("👋 **Ready!** Send files.\nModes: Standalone, Series (Auto/Caption), Not an Audiobook.")

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_command(client, message):
    uptime = str(datetime.timedelta(seconds=int(time.time() - START_TIME)))
    await message.reply_text(f"📊 **Stats**\n⏱️ Uptime: `{uptime}`\n📁 Files: `{TOTAL_FILES}`\n💾 Data: `{TOTAL_BYTES/1024/1024/1024:.2f} GB`")

@app.on_message(filters.media & filters.user(OWNER_ID))
async def handle_media(client, message: Message):
    try:
        media_type = message.media.value
        file_obj = getattr(message, media_type)
        filename = getattr(file_obj, 'file_name', f"{media_type}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.file")
        caption = message.caption or "" # Capture caption
        
        # Store file info with caption
        file_info = {'msg_id': message.id, 'name': filename, 'caption': caption}

        if message.media_group_id:
            group_id = message.media_group_id
            if group_id not in ALBUMS:
                ALBUMS[group_id] = []
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
                        # 1. Series (Caption OR Auto)
                        if detected_caption:
                            buttons.append([InlineKeyboardButton(f"📂 Series: {detected_caption[:30]}...", callback_data=f"cap|{key}")])
                        else:
                            buttons.append([InlineKeyboardButton(f"📂 Series: {cleaned_name[:30]}...", callback_data=f"auto|{key}")])
                        
                        # 2. Standalone
                        buttons.append([InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")])
                        # 3. Not an Audiobook
                        buttons.append([InlineKeyboardButton("🚫 Not an Audiobook (Root)", callback_data=f"root|{key}")])
                        # 4. Custom
                        buttons.append([InlineKeyboardButton("✏️ Custom Series Name", callback_data=f"custom|{key}")])
                        
                        txt = f"📦 **Album Detected**\n📚 Files: {len(file_list)}"
                        if detected_caption: txt += f"\n🏷 **Caption:** `{detected_caption}`"
                        await message.reply_text(txt, reply_markup=InlineKeyboardMarkup(buttons))
                asyncio.create_task(process_album())
            ALBUMS[group_id].append(file_info)
        else:
            key = f"single_{message.id}"
            TEMP_FILES[key] = [file_info]
            cleaned_name = clean_series_name(filename)
            
            buttons = []
            if caption:
                buttons.append([InlineKeyboardButton(f"📂 Series: {caption[:30]}...", callback_data=f"cap|{key}")])
            else:
                buttons.append([InlineKeyboardButton(f"📂 Series: {cleaned_name[:30]}...", callback_data=f"auto|{key}")])
                
            buttons.append([InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")])
            buttons.append([InlineKeyboardButton("🚫 Not an Audiobook (Root)", callback_data=f"root|{key}")])
            buttons.append([InlineKeyboardButton("✏️ Custom Series Name", callback_data=f"custom|{key}")])
            
            txt = f"📄 **File Received**\n`{filename}`"
            if caption: txt += f"\n🏷 **Caption:** `{caption}`"
            await message.reply_text(txt, reply_markup=InlineKeyboardMarkup(buttons))
            
    except Exception as e:
        logger.error(f"Error handling media: {e}")
        await message.reply_text(f"❌ Error: {str(e)}")

@app.on_callback_query()
async def handle_callback(client, query):
    try:
        data_parts = query.data.split('|')
        mode, key = data_parts[0], data_parts[1]
        
        if key not in TEMP_FILES:
            return await query.answer("❌ Session expired. Re-send files.", show_alert=True)
        
        file_list = TEMP_FILES[key]
        
        if mode == "std":
            await query.message.edit_text(f"🚀 Starting Standalone upload...")
            asyncio.create_task(upload_task(client, query.message, file_list, series_name=None))
            del TEMP_FILES[key]
        
        elif mode == "root":
            # Direct upload, no subfolders
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
            first_file = file_list[0]['name']
            series_name = clean_series_name(first_file)
            await query.message.edit_text(f"🚀 Series Upload: **{series_name}**")
            asyncio.create_task(upload_task(client, query.message, file_list, series_name=series_name))
            del TEMP_FILES[key]
            
        elif mode == "custom":
            ACTIVE_SERIES[query.from_user.id] = {'file_list': file_list, 'key': key}
            await query.message.edit_text("✏️ **Reply with Series Name:**")
            
        await query.answer()
    except Exception as e:
        logger.error(f"Callback error: {e}")
        await query.answer(f"Error: {str(e)}", show_alert=True)

@app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "stats"]))
async def handle_text(client, message: Message):
    if message.from_user.id in ACTIVE_SERIES:
        data = ACTIVE_SERIES.pop(message.from_user.id)
        if data['key'] in TEMP_FILES: del TEMP_FILES[data['key']]
        status = await message.reply_text(f"🚀 Series Upload: **{message.text.strip()}**")
        asyncio.create_task(upload_task(client, status, data['file_list'], series_name=message.text.strip()))

if __name__ == "__main__":
    os.makedirs("downloads", exist_ok=True)
    try:
        service = get_drive_service()
        if service: sync_stats(service, save=False)
    except: pass
    app.run()
