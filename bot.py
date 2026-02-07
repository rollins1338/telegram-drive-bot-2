import os
import json
import asyncio
import threading
import time
import datetime
import io
import re
import shutil
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
# EXACT ORIGINAL CODE - DO NOT TOUCH
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
# =============================================================

# ==================== CONFIGURATION ====================
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
BOT_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID', '')
OWNER_ID = int(os.getenv('OWNER_ID', '0'))
TOKEN_JSON = os.getenv('TOKEN_JSON', '')

# Global state for persistence
START_TIME = time.time()
TOTAL_FILES = 0
TOTAL_BYTES = 0
UPLOAD_QUEUE = {} 
FAILED_UPLOADS = {}
QUEUE_COUNTER = 0

ACTIVE_TASKS = {}
TEMP_FILES = {}

app = Client("gdrive_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ==================== DRIVE PERSISTENCE ====================

def get_drive_service():
    try:
        creds_data = json.loads(TOKEN_JSON)
        credentials = Credentials.from_authorized_user_info(creds_data)
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Drive Auth Error: {e}")
        return None

def sync_stats(service, save=False):
    """Saves/Loads stats and queues to Google Drive (Survives Koyeb Restarts)"""
    global TOTAL_FILES, TOTAL_BYTES, UPLOAD_QUEUE, FAILED_UPLOADS, QUEUE_COUNTER
    if not service: return
    
    filename = "bot_state_persistent.json"
    try:
        # Escaping for Python 3.11 query
        query = "name='" + filename + "' and '" + DRIVE_FOLDER_ID + "' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        file_id = files[0]['id'] if files else None

        if save:
            state = {
                "total_files": TOTAL_FILES,
                "total_bytes": TOTAL_BYTES,
                "queue_counter": QUEUE_COUNTER,
                "upload_queue": UPLOAD_QUEUE,
                "failed_uploads": FAILED_UPLOADS,
                "last_sync": datetime.datetime.now().isoformat()
            }
            media = MediaIoBaseUpload(io.BytesIO(json.dumps(state).encode()), mimetype='application/json')
            if file_id:
                service.files().update(fileId=file_id, media_body=media).execute()
            else:
                service.files().create(body={'name': filename, 'parents': [DRIVE_FOLDER_ID]}, media_body=media).execute()
        else:
            if file_id:
                request = service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done: _, done = downloader.next_chunk()
                
                state = json.loads(fh.getvalue().decode())
                TOTAL_FILES = state.get('total_files', 0)
                TOTAL_BYTES = state.get('total_bytes', 0)
                QUEUE_COUNTER = state.get('queue_counter', 0)
                UPLOAD_QUEUE = state.get('upload_queue', {})
                FAILED_UPLOADS = state.get('failed_uploads', {})
                logger.info("🧠 Persistent state loaded from Drive")
    except Exception as e:
        logger.error(f"Sync error: {e}")

# ==================== UTILS ====================

def check_disk_space(min_mb=1000):
    """Safety check for Koyeb ephemeral disk"""
    _, _, free = shutil.disk_usage("/")
    return (free // (2**20)) > min_mb

def format_size(b):
    for u in ['B','KB','MB','GB','TB']:
        if b < 1024: return f"{b:.2f} {u}"
        b /= 1024

def clean_filename(f):
    name, ext = os.path.splitext(f)
    return re.sub(r'\s+', ' ', name.replace('_', ' ')).strip() + ext

def clean_series_name(f):
    name = os.path.splitext(f)[0]
    name = re.sub(r'[\[\(].*?[\]\)]', '', name)
    name = re.sub(r'\b(1080p|720p|480p|BluRay|x264|x265|HEVC|S\d+E\d+|\d+x\d+)\b', '', name, flags=re.I)
    return re.sub(r'[._-]+', ' ', name).strip() or "Unknown Series"

# ==================== UPLOAD TASK ====================

async def upload_task(client, status_msg, file_list, series_name=None, flat_upload=False, queue_id=None):
    global TOTAL_FILES, TOTAL_BYTES
    task_id = f"{status_msg.chat.id}_{status_msg.id}"
    ACTIVE_TASKS[task_id] = {'cancelled': False}
    
    service = get_drive_service()
    if not service: return

    try:
        # Determine Folder
        parent_id = DRIVE_FOLDER_ID
        if series_name and not flat_upload:
            safe_sn = series_name.replace("'", "\\'")
            q = "name='" + safe_sn + "' and '" + DRIVE_FOLDER_ID + "' in parents and trashed=false"
            res = service.files().list(q=q).execute().get('files', [])
            if res:
                parent_id = res[0]['id']
            else:
                meta = {'name': series_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [DRIVE_FOLDER_ID]}
                parent_id = service.files().create(body=meta, fields='id').execute().get('id')

        os.makedirs("downloads", exist_ok=True)
        
        for idx, file_info in enumerate(file_list, 1):
            if ACTIVE_TASKS.get(task_id, {}).get('cancelled'): break
            
            if not check_disk_space():
                await status_msg.edit_text("❌ **Disk Full!** Skipping rest.")
                break

            filename = file_info['name']
            clean_name = clean_filename(filename)
            path = f"downloads/{filename}"

            await status_msg.edit_text(f"📥 **Downloading ({idx}/{len(file_list)})**\n`{filename}`")
            await client.download_media(message=await client.get_messages(status_msg.chat.id, file_info['msg_id']), file_name=path)
            
            # Target subfolder
            target_folder = parent_id
            if not flat_upload:
                sub_name = os.path.splitext(clean_name)[0]
                meta = {'name': sub_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
                target_folder = service.files().create(body=meta, fields='id').execute().get('id')

            # Upload
            media = MediaFileUpload(path, resumable=True)
            request = service.files().create(body={'name': clean_name, 'parents': [target_folder]}, media_body=media, fields='id, size')
            
            response = None
            while response is None:
                status, response = request.next_chunk()
                if status:
                    prog = int(status.progress() * 100)
                    if prog % 25 == 0: 
                        try: await status_msg.edit_text(f"☁️ **Uploading ({idx}/{len(file_list)})**\nProgress: {prog}%")
                        except: pass

            size = int(response.get('size', 0))
            TOTAL_FILES += 1
            TOTAL_BYTES += size
            if os.path.exists(path): os.remove(path)
            sync_stats(service, save=True)

        await status_msg.edit_text(f"✅ **Complete!**\nTotal: {format_size(TOTAL_BYTES)}")
        if queue_id in UPLOAD_QUEUE: del UPLOAD_QUEUE[queue_id]
        sync_stats(service, save=True)

    except Exception as e:
        logger.error(f"Task Error: {e}")
        FAILED_UPLOADS[task_id] = {'files': [f['name'] for f in file_list], 'error': str(e)}
        sync_stats(service, save=True)
    finally:
        if task_id in ACTIVE_TASKS: del ACTIVE_TASKS[task_id]

# ==================== HANDLERS ====================

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_handler(client, message):
    uptime = str(datetime.timedelta(seconds=int(time.time() - START_TIME)))
    await message.reply_text(f"📊 **Stats**\nUptime: {uptime}\nFiles: {TOTAL_FILES}\nData: {format_size(TOTAL_BYTES)}")

@app.on_message(filters.command("retry") & filters.user(OWNER_ID))
async def retry_handler(client, message):
    if not FAILED_UPLOADS: return await message.reply_text("✅ No failures.")
    text = "❌ **Failures:**\n" + "\n".join([f"• {v['files'][0]}" for k, v in list(FAILED_UPLOADS.items())[:5]])
    btn = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Clear Failures", callback_data="clear_f")]])
    await message.reply_text(text, reply_markup=btn)

@app.on_callback_query(filters.regex("clear_f"))
async def clear_callback(client, query):
    global FAILED_UPLOADS
    FAILED_UPLOADS = {}
    sync_stats(get_drive_service(), save=True)
    await query.message.edit_text("🗑️ Cleared.")

@app.on_message(filters.media & filters.user(OWNER_ID))
async def media_handler(client, message):
    file_obj = getattr(message, message.media.value)
    filename = getattr(file_obj, 'file_name', f"file_{message.id}")
    
    key = f"t_{message.id}"
    TEMP_FILES[key] = [{'msg_id': message.id, 'name': filename}]
    
    buttons = [
        [InlineKeyboardButton("📂 Series (Auto)", callback_data=f"auto|{key}")],
        [InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")],
        [InlineKeyboardButton("🚫 Root", callback_data=f"root|{key}")]
    ]
    await message.reply_text(f"📄 `{filename}`", reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex(r"^(auto|std|root)\|"))
async def process_selection(client, query):
    mode, key = query.data.split("|")
    files = TEMP_FILES.get(key)
    if not files: return await query.answer("Expired.")
    
    series = clean_series_name(files[0]['name']) if mode == "auto" else None
    flat = (mode == "root")
    
    global QUEUE_COUNTER
    QUEUE_COUNTER += 1
    qid = f"Q_{QUEUE_COUNTER}"
    UPLOAD_QUEUE[qid] = {'files': files, 'series': series, 'flat': flat}
    
    await query.message.edit_text("🚀 Starting...")
    sync_stats(get_drive_service(), save=True)
    asyncio.create_task(upload_task(client, query.message, files, series, flat, qid))

if __name__ == "__main__":
    service = get_drive_service()
    if service: sync_stats(service, save=False)
    app.run()
