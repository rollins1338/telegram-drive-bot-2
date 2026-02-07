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

# ==================== HEALTH CHECK SERVER (RESTORED EXACTLY) ====================
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

# ==================== CONFIGURATION (RESTORED EXACTLY) ====================
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
BOT_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID', '')
OWNER_ID = int(os.getenv('OWNER_ID', '0'))
TOKEN_JSON = os.getenv('TOKEN_JSON', '')

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

# Storage
ALBUMS = {}
TEMP_FILES = {}
ACTIVE_SERIES = {}
ACTIVE_TASKS = {}
UPLOAD_QUEUE = {} 
QUEUE_COUNTER = 0
FAILED_UPLOADS = {} 
MAX_RETRIES = 3
RETRY_DELAY = 5 

# ==================== DRIVE FUNCTIONS (ORIGINAL - UNTOUCHED) ====================
def get_drive_service():
    try:
        creds_data = json.loads(TOKEN_JSON)
        credentials = Credentials.from_authorized_user_info(creds_data)
        service = build('drive', 'v3', credentials=credentials)
        return service
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
            stats_data = json.dumps({
                "total_files": TOTAL_FILES,
                "total_bytes": TOTAL_BYTES,
                "last_updated": datetime.datetime.now().isoformat()
            })
            media = MediaIoBaseUpload(io.BytesIO(stats_data.encode()), mimetype='application/json')
            if file_id:
                service.files().update(fileId=file_id, media_body=media).execute()
            else:
                service.files().create(body={'name': stats_filename, 'parents': [DRIVE_FOLDER_ID]}, media_body=media).execute()
        else:
            if file_id:
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
    except Exception as e:
        logger.error(f"Error creating folder: {e}")
        return None

# ==================== FILENAME HELPERS (ORIGINAL - UNTOUCHED) ====================
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

# ==================== UPLOAD CORE ENGINE (ORIGINAL - UNTOUCHED) ====================
def add_to_queue(file_list, series_name=None, flat_upload=False):
    global QUEUE_COUNTER
    QUEUE_COUNTER += 1
    queue_id = f"queue_{QUEUE_COUNTER}"
    UPLOAD_QUEUE[queue_id] = {'files': file_list, 'series_name': series_name, 'flat_upload': flat_upload, 'status': 'pending', 'created_at': time.time(), 'priority': QUEUE_COUNTER}
    return queue_id

async def progress_callback(current, total, message, start_time, filename, task_id):
    if ACTIVE_TASKS.get(task_id, {}).get('cancelled', False): raise Exception("Download cancelled by user")
    now = time.time()
    if not hasattr(message, 'last_update'): message.last_update, message.last_current, message.last_time = 0, 0, start_time
    if (now - message.last_update) > 2 or current == total:
        time_diff = now - message.last_time
        speed = (current - message.last_current) / time_diff if time_diff > 0 else 0
        percentage = (current / total) * 100
        bar = '■' * int(percentage // 10) + '□' * (10 - int(percentage // 10))
        eta = str(datetime.timedelta(seconds=int((total - current) / speed))) if speed > 0 and current < total else "..."
        try:
            cb = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Cancel Upload", callback_data=f"cancel_{task_id}")]])
            await message.edit_text(f"📊 **Progress**\n📄 `{filename[:40]}...`\n\n[{bar}] {percentage:.1f}%\n⚡ {format_size(speed)}/s\n💾 {format_size(current)} / {format_size(total)}\n⏱️ ETA: {eta}", reply_markup=cb)
        except: pass
        message.last_update, message.last_current, message.last_time = now, current, now

def upload_to_drive_with_progress(service, file_path, file_metadata, message_data, filename):
    try:
        media = MediaFileUpload(file_path, resumable=True, chunksize=5*1024*1024)
        request = service.files().create(body=file_metadata, media_body=media, fields='id, size')
        response, last_progress, last_update, last_uploaded = None, 0, time.time(), 0
        file_size = os.path.getsize(file_path)
        while response is None:
            task_id = message_data.get('task_id')
            if task_id and ACTIVE_TASKS.get(task_id, {}).get('cancelled', False):
                message_data['cancelled'] = True
                return None
            status, response = request.next_chunk()
            if status:
                p, c = int(status.progress() * 100), int(status.progress() * file_size)
                now = time.time()
                if (now - last_update) > 2 or p - last_progress >= 5:
                    speed = (c - last_uploaded) / (now - last_update) if (now - last_update) > 0 else 0
                    bar = '■' * int(p // 10) + '□' * (10 - int(p // 10))
                    eta = str(datetime.timedelta(seconds=int((file_size - c) / speed))) if speed > 0 else "..."
                    message_data['last_progress'] = {'bar': bar, 'progress': p, 'speed': speed, 'current': c, 'total': file_size, 'eta': eta, 'filename': filename}
                    last_progress, last_update, last_uploaded = p, now, c
        message_data['complete'] = True
        return response
    except Exception as e: message_data['error'] = str(e); raise

async def upload_task(client, status_msg, file_list, series_name=None, flat_upload=False, queue_id=None):
    global TOTAL_FILES, TOTAL_BYTES
    task_id = f"{status_msg.chat.id}_{status_msg.id}"
    ACTIVE_TASKS[task_id] = {'cancelled': False, 'files_list': file_list, 'current_file': None, 'progress': 0, 'status': 'initializing'}
    service = get_drive_service()
    if not service:
        await status_msg.edit_text("❌ Connection failed."); return
    try:
        parent_folder = DRIVE_FOLDER_ID
        if series_name and not flat_upload:
            parent_folder = get_or_create_folder(service, series_name, DRIVE_FOLDER_ID)
        os.makedirs("downloads", exist_ok=True)
        succ, tot_sz, start_t = 0, 0, time.time()
        for idx, file_info in enumerate(file_list, 1):
            if ACTIVE_TASKS[task_id]['cancelled']: break
            filename = file_info['name']
            clean_n = clean_filename(filename)
            d_path = f"downloads/{filename}"
            ACTIVE_TASKS[task_id].update({'current_file': filename, 'status': 'downloading'})
            
            # Download Logic
            msg = await client.get_messages(status_msg.chat.id, file_info['msg_id'])
            await client.download_media(msg, file_name=d_path, progress=progress_callback, progress_args=(status_msg, time.time(), filename, task_id))
            
            if ACTIVE_TASKS[task_id]['cancelled']: break
            
            # Folder Logic
            target = parent_folder if flat_upload else get_or_create_folder(service, os.path.splitext(clean_n)[0], parent_folder)
            
            # Sophisticated Upload Loop
            prog_data = {'complete': False, 'error': None, 'last_progress': None, 'task_id': task_id, 'cancelled': False}
            loop = asyncio.get_running_loop()
            up_fut = loop.run_in_executor(None, upload_to_drive_with_progress, service, d_path, {'name': clean_n, 'parents': [target]}, prog_data, filename)
            
            while not prog_data['complete'] and not prog_data['error'] and not prog_data['cancelled']:
                if ACTIVE_TASKS[task_id]['cancelled']: prog_data['cancelled'] = True; break
                if prog_data['last_progress']:
                    p = prog_data['last_progress']
                    try: await status_msg.edit_text(f"☁️ **Uploading ({idx}/{len(file_list)})**\n📄 `{p['filename'][:35]}...`\n\n[{p['bar']}] {p['progress']}%\n⚡ {format_size(p['speed'])}/s\n💾 {format_size(p['current'])} / {format_size(p['total'])}\n⏱️ ETA: {p['eta']}\n\n✅ {succ} Done | Overall: {int((idx-1)/len(file_list)*100)}%")
                    except: pass
                await asyncio.sleep(1)
            
            res = await up_fut
            if os.path.exists(d_path): os.remove(d_path)
            if res:
                size = int(res.get('size', 0))
                TOTAL_FILES += 1; TOTAL_BYTES += size; tot_sz += size; succ += 1
        
        await status_msg.edit_text(f"✅ **Complete!**\nFiles: {succ}/{len(file_list)}\nSize: {format_size(tot_sz)}\nTime: {format_time(time.time()-start_t)}")
        sync_stats(service, save=True)
    except Exception as e: await status_msg.edit_text(f"❌ Error: {e}")
    finally: ACTIVE_TASKS.pop(task_id, None)

# ==================== DRIVE BROWSER & MIRROR (DOWNLOAD MODE ADDED) ====================
async def get_browser_menu(service, fid='root', pname="Root"):
    try:
        q = f"'{fid}' in parents and trashed=false"
        res = service.files().list(q=q, fields="files(id, name, mimeType, size)", orderBy="folder, name").execute().get('files', [])
        btns = []
        for f in res:
            if f['mimeType'] == 'application/vnd.google-apps.folder':
                btns.append([InlineKeyboardButton("📁 " + f['name'], callback_data=f"brw|{f['id']}")])
            else:
                btns.append([InlineKeyboardButton("📄 " + f['name'] + " (" + format_size(int(f.get('size', 0))) + ")", callback_data=f"mir|{f['id']}")])
        
        pid = 'root'
        if fid != 'root':
            parents = service.files().get(fileId=fid, fields='parents').execute().get('parents', [])
            pid = parents[0] if parents else 'root'
        btns.append([InlineKeyboardButton("⬅️ Back", callback_data=f"brw|{pid}"), InlineKeyboardButton("🏠 Main Menu", callback_data="goto_start")])
        return f"📍 **Path:** `{pname}`", InlineKeyboardMarkup(btns)
    except: return "❌ Error fetching Drive.", None

async def download_mirror_task(client, query, fid):
    service = get_drive_service()
    try:
        meta = service.files().get(fileId=fid, fields='name, size').execute()
        name, size = meta['name'], int(meta.get('size', 0))
        msg = await query.message.edit_text(f"⏳ **Mirroring...**\n`{name}`\nDownloading...")
        
        os.makedirs("mirrors", exist_ok=True)
        path = os.path.join("mirrors", name)
        request = service.files().get_media(fileId=fid)
        with io.FileIO(path, 'wb') as fh:
            dl = MediaIoBaseDownload(fh, request, chunksize=10*1024*1024)
            done = False
            while not done:
                st, done = dl.next_chunk()
                if st: await msg.edit_text(f"⏳ **Mirroring...** {int(st.progress()*100)}%")
        
        await msg.edit_text("⏳ **Mirroring...**\nSending to Telegram...")
        if name.lower().endswith(('.mp3', '.m4b')):
            await client.send_audio(query.message.chat.id, audio=path, caption=f"✅ `{name}`")
        else:
            await client.send_document(query.message.chat.id, document=path, caption=f"✅ `{name}`")
        
        await msg.delete()
        if os.path.exists(path): os.remove(path)
    except Exception as e: await query.message.edit_text(f"❌ Error: {e}")

# ==================== SOPHISTICATED HANDLERS (ORIGINAL - UNTOUCHED) ====================

@app.on_message(filters.command("start") & filters.user(OWNER_ID))
async def cmd_start(client, message):
    btns = [[InlineKeyboardButton("📤 Upload Mode", callback_data="setup_up")], [InlineKeyboardButton("📥 Download Mode", callback_data="setup_down")]]
    await message.reply_text("🤖 **RxUploader Pro**\nSelect operation mode:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex("goto_start"))
async def goto_start_cb(client, query):
    await cmd_start(client, query.message)

@app.on_callback_query(filters.regex("setup_up"))
async def setup_up_cb(client, query):
    await query.message.edit_text("📤 **Upload Mode Enabled**\nSend or forward media/albums to start organization.")

@app.on_callback_query(filters.regex("setup_down"))
async def setup_down_cb(client, query):
    s = get_drive_service()
    t, m = await get_browser_menu(s)
    await query.message.edit_text(t, reply_markup=m)

@app.on_callback_query(filters.regex(r"^brw\|"))
async def browser_cb(client, query):
    fid = query.data.split("|")[1]
    s = get_drive_service()
    pn = "Root" if fid == "root" else s.files().get(fileId=fid, fields='name').execute().get('name', 'Folder')
    t, m = await get_browser_menu(s, fid, pn)
    await query.message.edit_text(t, reply_markup=m)

@app.on_callback_query(filters.regex(r"^mir\|"))
async def mirror_cb(client, query):
    await download_mirror_task(client, query, query.data.split("|")[1])

@app.on_message(filters.media & filters.user(OWNER_ID))
async def handle_media_sophisticated(client, message):
    m_type = message.media.value
    f_obj = getattr(message, m_type)
    fname = getattr(f_obj, 'file_name', f"file_{message.id}")
    cap = message.caption or ""
    info = {'msg_id': message.id, 'name': fname, 'caption': cap}
    
    if message.media_group_id:
        gid = message.media_group_id
        if gid not in ALBUMS:
            ALBUMS[gid] = []
            async def process_alb():
                await asyncio.sleep(2)
                if gid not in ALBUMS: return
                flist = ALBUMS.pop(gid)
                key = f"alb_{gid}"
                TEMP_FILES[key] = flist
                d_cap = next((f['caption'] for f in flist if f['caption']), None)
                cln_n = clean_series_name(flist[0]['name'])
                btns = []
                if d_cap: btns.append([InlineKeyboardButton(f"📂 Series: {d_cap[:25]}", callback_data=f"cap|{key}")])
                else: btns.append([InlineKeyboardButton(f"📂 Series: {cln_n[:25]}", callback_data=f"auto|{key}")])
                btns.extend([[InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")], [InlineKeyboardButton("✏️ Custom", callback_data=f"custom|{key}")], [InlineKeyboardButton("🚫 Not Audiobook", callback_data=f"root|{key}")]])
                await message.reply_text(f"📦 **Album Detected** ({len(flist)} files)", reply_markup=InlineKeyboardMarkup(btns))
            asyncio.create_task(process_alb())
        ALBUMS[gid].append(info)
    else:
        key = f"sig_{message.id}"; TEMP_FILES[key] = [info]
        cln_n = clean_series_name(fname)
        btns = []
        if cap: btns.append([InlineKeyboardButton(f"📂 Series: {cap[:25]}", callback_data=f"cap|{key}")])
        else: btns.append([InlineKeyboardButton(f"📂 Series: {cln_n[:25]}", callback_data=f"auto|{key}")])
        btns.extend([[InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")], [InlineKeyboardButton("✏️ Custom", callback_data=f"custom|{key}")], [InlineKeyboardButton("🚫 Not Audiobook", callback_data=f"root|{key}")]])
        await message.reply_text(f"📄 **File:** `{fname}`", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^(std|root|cap|auto|custom)\|"))
async def up_selection_cb(client, query):
    mode, key = query.data.split("|")
    files = TEMP_FILES.get(key)
    if not files: return await query.answer("Session Expired.")
    if mode == "custom":
        ACTIVE_SERIES[query.from_user.id] = {'files': files, 'key': key}
        return await query.message.edit_text("✏️ Send the **Custom Series Name**:")
    series = clean_series_name(files[0]['name']) if mode == "auto" else (next((f['caption'] for f in files if f['caption']), "Unknown") if mode == "cap" else None)
    flat = (mode == "root")
    await query.message.edit_text("🚀 Starting upload task...")
    asyncio.create_task(upload_task(client, query.message, files, series, flat))
    TEMP_FILES.pop(key, None)

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def cmd_stats(client, message):
    uptime = str(datetime.timedelta(seconds=int(time.time() - START_TIME)))
    text = f"📊 **Stats**\nUptime: {uptime}\nTotal Uploaded: {TOTAL_FILES} files | {format_size(TOTAL_BYTES)}"
    await message.reply_text(text)

@app.on_message(filters.command("cancel") & filters.user(OWNER_ID))
async def cmd_cancel(client, message):
    for tid in list(ACTIVE_TASKS.keys()): ACTIVE_TASKS[tid]['cancelled'] = True
    await message.reply_text("🛑 Cancellation requested for all tasks.")

# Additional stats/queue/status command support
@app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "stats", "cancel"]))
async def custom_name_cb(client, message):
    if message.from_user.id in ACTIVE_SERIES:
        data = ACTIVE_SERIES.pop(message.from_user.id)
        status = await message.reply_text(f"🚀 Series Upload: **{message.text}**")
        asyncio.create_task(upload_task(client, status, data['files'], series_name=message.text))

if __name__ == "__main__":
    service = get_drive_service()
    if service: sync_stats(service, save=False)
    app.run()
