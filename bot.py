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
                pass
        HTTPServer(('0.0.0.0', 8000), HealthHandler).serve_forever()
    except Exception as e:
        logger.error(f"Health server error: {e}")

threading.Thread(target=run_health_server, daemon=True).start()

# ==================== CONFIGURATION ====================
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
BOT_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID', '')
OWNER_ID = int(os.getenv('OWNER_ID', '0'))
TOKEN_JSON = os.getenv('TOKEN_JSON', '')

# Global stats and state (Restored from Original)
START_TIME = time.time()
TOTAL_FILES = 0
TOTAL_BYTES = 0
ALBUMS = {}
TEMP_FILES = {}
ACTIVE_SERIES = {}
ACTIVE_TASKS = {}
UPLOAD_QUEUE = {}
QUEUE_COUNTER = 0
FAILED_UPLOADS = {}
MAX_RETRIES = 3
RETRY_DELAY = 5

app = Client("gdrive_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, ipv6=False)

# ==================== RESTORED ORIGINAL FUNCTIONS ====================

def get_drive_service():
    try:
        creds_data = json.loads(TOKEN_JSON)
        credentials = Credentials.from_authorized_user_info(creds_data)
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Error creating Drive service: {e}")
        return None

def sync_stats(service, save=False):
    global TOTAL_FILES, TOTAL_BYTES
    if not service: return
    stats_filename = "bot_stats.json"
    try:
        query = f"name='{stats_filename}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        file_id = files[0]['id'] if files else None
        if save:
            stats_data = json.dumps({"total_files": TOTAL_FILES, "total_bytes": TOTAL_BYTES, "last_updated": datetime.datetime.now().isoformat()})
            media = MediaIoBaseUpload(io.BytesIO(stats_data.encode()), mimetype='application/json')
            if file_id: service.files().update(fileId=file_id, media_body=media).execute()
            else: service.files().create(body={'name': stats_filename, 'parents': [DRIVE_FOLDER_ID]}, media_body=media).execute()
        elif file_id:
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done: _, done = downloader.next_chunk()
            stats = json.loads(fh.getvalue().decode())
            TOTAL_FILES, TOTAL_BYTES = stats.get('total_files', 0), stats.get('total_bytes', 0)
    except Exception as e: logger.error(f"Error syncing stats: {e}")

def get_or_create_folder(service, folder_name, parent_id):
    try:
        clean_name = folder_name.replace('"', '\\"')
        query = f'name="{clean_name}" and "{parent_id}" in parents and mimeType="application/vnd.google-apps.folder" and trashed=false'
        results = service.files().list(q=query, fields="files(id, name)").execute()
        folders = results.get('files', [])
        if folders: return folders[0]['id']
        file_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
        folder = service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')
    except Exception as e: return None

def clean_series_name(filename):
    name = os.path.splitext(filename)[0]
    name = re.sub(r'[\[\(][^\]\)]*[\]\)]', '', name)
    name = re.sub(r'\b(1080p|720p|480p|2160p|4K|UHD|HDR|BluRay|BRRip|WEBRip|WEBDL|WEB-DL|DVDRip|HDTV|x264|x265|HEVC|AAC|AC3|DTS|DD5\.1|10bit|8bit)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(S\d+E\d+|s\d+e\d+|\d+x\d+|Episode?\s*\d+|Ep?\s*\d+|E\d+|Ch\d+|Chapter\s*\d+)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[._-]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name or "Unknown Series"

def clean_filename(filename):
    name, ext = os.path.splitext(filename)
    name = name.replace('_', ' ')
    name = re.sub(r'\s+', ' ', name).strip()
    return name + ext

def format_size(bytes_size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0: return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

def format_time(seconds):
    if seconds < 60: return f"{int(seconds)}s"
    elif seconds < 3600: return f"{int(seconds/60)}m {int(seconds%60)}s"
    else: return f"{int(seconds/3600)}h {int((seconds%3600)/60)}m"

# ==================== RESTORED ORIGINAL UPLOAD ENGINE ====================

async def progress_callback(current, total, message, start_time, filename, task_id):
    if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False): raise Exception("Download cancelled by user")
    now = time.time()
    if not hasattr(message, 'last_update'):
        message.last_update = 0
        message.last_current = 0
        message.last_time = start_time
    if (now - message.last_update) > 2 or current == total:
        time_diff = now - message.last_time
        bytes_diff = current - message.last_current
        speed = bytes_diff / time_diff if time_diff > 0 else 0
        percentage = (current / total) * 100
        filled = int(percentage // 10)
        bar = '■' * filled + '□' * (10 - filled)
        if speed > 0 and current < total:
            eta = str(datetime.timedelta(seconds=int((total - current) / speed)))
        else: eta = "Calculating..." if current < total else "Done"
        try:
            btn = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Cancel Upload", callback_data=f"cancel_{task_id}")]])
            await message.edit_text(f"📊 **Progress**\n📄 `{filename[:40]}...`\n\n[{bar}] {percentage:.1f}%\n⚡ Speed: {speed/1024/1024:.2f} MB/s\n💾 {current/1024/1024:.1f} MB / {total/1024/1024:.1f} MB\n⏱️ ETA: {eta}", reply_markup=btn)
        except: pass
        message.last_update, message.last_current, message.last_time = now, current, now

def upload_to_drive_with_progress(service, file_path, file_metadata, message_data, filename):
    try:
        media = MediaFileUpload(file_path, resumable=True, chunksize=5*1024*1024)
        request = service.files().create(body=file_metadata, media_body=media, fields='id, size')
        response = None
        last_progress, last_update, last_uploaded = 0, time.time(), 0
        file_size = os.path.getsize(file_path)
        while response is None:
            task_id = message_data.get('task_id')
            if task_id and ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                message_data['cancelled'] = True
                return None
            status, response = request.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                current_bytes = int(status.progress() * file_size)
                now = time.time()
                if (now - last_update) > 2 or progress - last_progress >= 5:
                    speed = (current_bytes - last_uploaded) / (now - last_update) if (now - last_update) > 0 else 0
                    filled = int(progress // 10)
                    bar = '■' * filled + '□' * (10 - filled)
                    eta = str(datetime.timedelta(seconds=int((file_size - current_bytes) / speed))) if speed > 0 else "N/A"
                    message_data['last_progress'] = {'bar': bar, 'progress': progress, 'speed': speed, 'current': current_bytes, 'total': file_size, 'eta': eta, 'filename': filename}
                    last_progress, last_update, last_uploaded = progress, now, current_bytes
        message_data['complete'] = True
        return response
    except Exception as e:
        message_data['error'] = str(e)
        raise

async def upload_task(client, status_msg, file_list, series_name=None, flat_upload=False, queue_id=None):
    global TOTAL_FILES, TOTAL_BYTES
    task_id = f"{status_msg.chat.id}_{status_msg.id}"
    ACTIVE_TASKS[task_id] = {'cancelled': False, 'files_list': file_list, 'current_file': None, 'progress': 0, 'status': 'initializing'}
    service = get_drive_service()
    if not service:
        await status_msg.edit_text("❌ Failed to connect to Drive.")
        return
    try:
        parent_folder = DRIVE_FOLDER_ID
        if series_name and not flat_upload:
            parent_folder = get_or_create_folder(service, series_name, DRIVE_FOLDER_ID)
        os.makedirs("downloads", exist_ok=True)
        successful_uploads, total_size_uploaded, start_time = 0, 0, time.time()
        
        for idx, file_info in enumerate(file_list, 1):
            if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False): break
            filename = file_info['name']
            clean_name = clean_filename(filename)
            download_path = f"downloads/{filename}"
            ACTIVE_TASKS[task_id].update({'current_file': filename, 'progress': int((idx - 1) / len(file_list) * 100), 'status': 'downloading'})
            
            # Download
            await client.download_media(message=await client.get_messages(status_msg.chat.id, file_info['msg_id']), file_name=download_path, progress=progress_callback, progress_args=(status_msg, time.time(), filename, task_id))
            
            if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                if os.path.exists(download_path): os.remove(download_path)
                break
                
            # Upload folder logic
            upload_folder = parent_folder if flat_upload else get_or_create_folder(service, os.path.splitext(clean_name)[0], parent_folder)
            
            # Upload
            progress_data = {'complete': False, 'error': None, 'last_progress': None, 'task_id': task_id, 'cancelled': False}
            loop = asyncio.get_running_loop()
            upload_future = loop.run_in_executor(None, upload_to_drive_with_progress, service, download_path, {'name': clean_name, 'parents': [upload_folder]}, progress_data, filename)
            
            while not progress_data['complete'] and not progress_data['error'] and not progress_data['cancelled']:
                if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                    progress_data['cancelled'] = True
                    break
                if progress_data['last_progress']:
                    p = progress_data['last_progress']
                    try: await status_msg.edit_text(f"☁️ **Uploading to Drive ({idx}/{len(file_list)})**\n📄 `{p['filename'][:40]}...`\n\n[{p['bar']}] {p['progress']}%\n⚡ Speed: {p['speed']/1024/1024:.2f} MB/s\n💾 {p['current']/1024/1024:.1f} MB / {p['total']/1024/1024:.1f} MB\n⏱️ ETA: {p['eta']}\n\n✅ {successful_uploads} uploaded | Overall: {int((idx-1)/len(file_list)*100)}%")
                    except: pass
                await asyncio.sleep(1)

            upload_result = await upload_future
            if os.path.exists(download_path): os.remove(download_path)
            if upload_result:
                size = int(upload_result.get('size', 0))
                TOTAL_FILES += 1
                TOTAL_BYTES += size
                total_size_uploaded += size
                successful_uploads += 1
        
        await status_msg.edit_text(f"✅ **Upload Complete!**\nFiles: {successful_uploads}/{len(file_list)}\nSize: {format_size(total_size_uploaded)}\nTime: {format_time(time.time() - start_time)}")
        sync_stats(service, save=True)
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
    finally: ACTIVE_TASKS.pop(task_id, None)

# ==================== DRIVE BROWSER (DOWNLOAD MODE) ====================

async def get_browser_menu(service, fid='root', pname="Root"):
    try:
        q = f"'{fid}' in parents and trashed=false"
        res = service.files().list(q=q, fields="files(id, name, mimeType, size)", orderBy="folder, name").execute().get('files', [])
        btns = []
        for f in res:
            if f['mimeType'] == 'application/vnd.google-apps.folder': btns.append([InlineKeyboardButton(f"📁 {f['name']}", callback_data=f"brw|{f['id']}")])
            else: btns.append([InlineKeyboardButton(f"📄 {f['name']} ({format_size(int(f.get('size', 0)))})", callback_data=f"mir|{f['id']}")])
        pid = service.files().get(fileId=fid, fields='parents').execute().get('parents', ['root'])[0] if fid != 'root' else 'root'
        btns.append([InlineKeyboardButton("⬅️ Back", callback_data=f"brw|{pid}"), InlineKeyboardButton("🏠 Menu", callback_data="back_start")])
        return f"📂 **Browser**\n📍 Path: `{pname}`", InlineKeyboardMarkup(btns)
    except: return "❌ Browser error.", None

async def mirror_file(client, query, fid):
    service = get_drive_service()
    try:
        meta = service.files().get(fileId=fid, fields='name, size').execute()
        name, size = meta['name'], int(meta.get('size', 0))
        msg = await query.message.edit_text(f"⏳ **Mirroring...**\n`{name}`")
        os.makedirs("mirrors", exist_ok=True)
        path = f"mirrors/{name}"
        request = service.files().get_media(fileId=fid)
        with io.FileIO(path, 'wb') as f:
            dl = MediaIoBaseDownload(f, request, chunksize=10*1024*1024)
            done = False
            while not done:
                st, done = dl.next_chunk()
                if st: await msg.edit_text(f"⏳ **Mirroring...** {int(st.progress()*100)}%")
        if name.lower().endswith(('.mp3', '.m4b')): await client.send_audio(query.message.chat.id, audio=path, caption=f"✅ `{name}`")
        else: await client.send_document(query.message.chat.id, document=path, caption=f"✅ `{name}`")
        await msg.delete()
        if os.path.exists(path): os.remove(path)
    except Exception as e: await query.message.edit_text(f"❌ Mirror Error: {e}")

# ==================== HANDLERS ====================

@app.on_message(filters.command("start") & filters.user(OWNER_ID))
async def start_cmd(client, message):
    btns = [[InlineKeyboardButton("📤 Upload Mode", callback_data="m_up")], [InlineKeyboardButton("📥 Download Mode", callback_data="m_down")]]
    await message.reply_text("🤖 **Main Menu**\nSelect a mode:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex("back_start"))
async def back_start(client, query): await start_cmd(client, query.message)

@app.on_callback_query(filters.regex("m_up"))
async def m_up_handler(client, query):
    await query.message.edit_text("📤 **Upload Mode Active**\n\nSend or forward any media files. I will handle them using the sophisticated album logic.")

@app.on_callback_query(filters.regex("m_down"))
async def m_down_handler(client, query):
    t, m = await get_browser_menu(get_drive_service())
    await query.message.edit_text(t, reply_markup=m)

@app.on_callback_query(filters.regex(r"^brw\|"))
async def brw_cb(client, query):
    fid = query.data.split("|")[1]
    s = get_drive_service()
    pn = "Root" if fid == 'root' else s.files().get(fileId=fid, fields='name').execute().get('name', 'Folder')
    t, m = await get_browser_menu(s, fid, pn)
    await query.message.edit_text(t, reply_markup=m)

@app.on_callback_query(filters.regex(r"^mir\|"))
async def mir_cb(client, query): await mirror_file(client, query, query.data.split("|")[1])

@app.on_callback_query(filters.regex(r"^(cap|auto|std|root|custom)\|"))
async def up_callback(client, query):
    mode, key = query.data.split("|")
    files = TEMP_FILES.get(key)
    if not files: return await query.answer("Session Expired.")
    if mode == "custom":
        ACTIVE_SERIES[query.from_user.id] = {'file_list': files, 'key': key}
        return await query.message.edit_text("✏️ Reply with the Custom Series Name:")
    series = None
    flat = False
    if mode == "cap": series = next((f['caption'] for f in files if f['caption']), "Unknown")
    elif mode == "auto": series = clean_series_name(files[0]['name'])
    elif mode == "root": flat = True
    await query.message.edit_text("🚀 Starting upload task...")
    asyncio.create_task(upload_task(client, query.message, files, series, flat))
    TEMP_FILES.pop(key, None)

@app.on_message(filters.media & filters.user(OWNER_ID))
async def media_handler(client, message):
    m_type = message.media.value
    f_obj = getattr(message, m_type)
    fname = getattr(f_obj, 'file_name', f"file_{message.id}")
    cap = message.caption or ""
    info = {'msg_id': message.id, 'name': fname, 'caption': cap}
    if message.media_group_id:
        gid = message.media_group_id
        if gid not in ALBUMS:
            ALBUMS[gid] = []
            async def process_album():
                await asyncio.sleep(2)
                if gid not in ALBUMS: return
                f_list = ALBUMS.pop(gid)
                key = f"alb_{gid}"
                TEMP_FILES[key] = f_list
                det_cap = next((f['caption'] for f in f_list if f['caption']), None)
                cl_name = clean_series_name(f_list[0]['name'])
                btns = []
                if det_cap: btns.append([InlineKeyboardButton(f"📂 Series: {det_cap[:25]}", callback_data=f"cap|{key}")])
                else: btns.append([InlineKeyboardButton(f"📂 Series: {cl_name[:25]}", callback_data=f"auto|{key}")])
                btns.extend([[InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")], [InlineKeyboardButton("🚫 Not Audiobook", callback_data=f"root|{key}")], [InlineKeyboardButton("✏️ Custom", callback_data=f"custom|{key}")]])
                await message.reply_text(f"📦 **Album Detected** ({len(f_list)} files)", reply_markup=InlineKeyboardMarkup(btns))
            asyncio.create_task(process_album())
        ALBUMS[gid].append(info)
    else:
        key = f"sig_{message.id}"
        TEMP_FILES[key] = [info]
        btns = []
        cl_name = clean_series_name(fname)
        if cap: btns.append([InlineKeyboardButton(f"📂 Series: {cap[:25]}", callback_data=f"cap|{key}")])
        else: btns.append([InlineKeyboardButton(f"📂 Series: {cl_name[:25]}", callback_data=f"auto|{key}")])
        btns.extend([[InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")], [InlineKeyboardButton("✏️ Custom", callback_data=f"custom|{key}")], [InlineKeyboardButton("🚫 Not Audiobook", callback_data=f"root|{key}")]])
        await message.reply_text(f"📄 **File:** `{fname}`", reply_markup=InlineKeyboardMarkup(btns))

@app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start"]))
async def text_handler(client, message):
    if message.from_user.id in ACTIVE_SERIES:
        data = ACTIVE_SERIES.pop(message.from_user.id)
        st = await message.reply_text(f"🚀 Series Upload: **{message.text}**")
        asyncio.create_task(upload_task(client, st, data['file_list'], series_name=message.text))

if __name__ == "__main__":
    service = get_drive_service()
    if service: sync_stats(service, save=False)
    app.run()
