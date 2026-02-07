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

# Global stats and state
START_TIME = time.time()
TOTAL_FILES = 0
TOTAL_BYTES = 0
ALBUMS = {}
TEMP_FILES = {}
ACTIVE_SERIES = {}
ACTIVE_TASKS = {}

app = Client("gdrive_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ==================== DRIVE SERVICE & SYNC ====================

def get_drive_service():
    try:
        creds_data = json.loads(TOKEN_JSON)
        credentials = Credentials.from_authorized_user_info(creds_data)
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Drive Auth Error: {e}")
        return None

def sync_stats(service, save=False):
    global TOTAL_FILES, TOTAL_BYTES
    if not service: return
    fn = "bot_stats.json"
    try:
        q = "name='" + fn + "' and '" + DRIVE_FOLDER_ID + "' in parents and trashed=false"
        res = service.files().list(q=q, fields="files(id)").execute()
        f_id = res.get('files', [{}])[0].get('id')
        if save:
            data = json.dumps({"total_files": TOTAL_FILES, "total_bytes": TOTAL_BYTES})
            media = MediaIoBaseUpload(io.BytesIO(data.encode()), mimetype='application/json')
            if f_id: service.files().update(fileId=f_id, media_body=media).execute()
            else: service.files().create(body={'name': fn, 'parents': [DRIVE_FOLDER_ID]}, media_body=media).execute()
        elif f_id:
            request = service.files().get_media(fileId=f_id)
            fh = io.BytesIO()
            dl = MediaIoBaseDownload(fh, request)
            done = False
            while not done: _, done = dl.next_chunk()
            stats = json.loads(fh.getvalue().decode())
            TOTAL_FILES, TOTAL_BYTES = stats.get('total_files', 0), stats.get('total_bytes', 0)
    except: pass

def get_or_create_folder(service, name, parent):
    try:
        safe_name = name.replace("'", "\\'")
        q = "name='" + safe_name + "' and '" + parent + "' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        res = service.files().list(q=q, fields="files(id)").execute().get('files', [])
        if res: return res[0]['id']
        meta = {'name': name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent]}
        return service.files().create(body=meta, fields='id').execute().get('id')
    except: return None

# ==================== ORIGINAL REGEX HELPERS ====================

def clean_series_name(f):
    name = os.path.splitext(f)[0]
    name = re.sub(r'[\[\(][^\]\)]*[\]\)]', '', name)
    name = re.sub(r'\b(1080p|720p|480p|2160p|4K|UHD|HDR|BluRay|BRRip|WEBRip|WEBDL|WEB-DL|DVDRip|HDTV|x264|x265|HEVC|AAC|AC3|DTS|DD5\.1|10bit|8bit)\b', '', name, flags=re.I)
    name = re.sub(r'\b(S\d+E\d+|s\d+e\d+|\d+x\d+|Episode?\s*\d+|Ep?\s*\d+|E\d+|Ch\d+|Chapter\s*\d+)\b', '', name, flags=re.I)
    return re.sub(r'[._-]+', ' ', name).strip() or "Unknown Series"

def clean_filename(f):
    n, e = os.path.splitext(f)
    return re.sub(r'\s+', ' ', n.replace('_', ' ')).strip() + e

def format_size(b):
    for u in ['B','KB','MB','GB','TB']:
        if b < 1024: return f"{b:.2f} {u}"
        b /= 1024

# ==================== SOPHISTICATED UPLOAD TASK ====================

async def upload_task(client, status_msg, file_list, series_name=None, flat_upload=False):
    global TOTAL_FILES, TOTAL_BYTES
    task_id = f"{status_msg.chat.id}_{status_msg.id}"
    ACTIVE_TASKS[task_id] = {'cancelled': False}
    service = get_drive_service()
    
    try:
        parent_folder = DRIVE_FOLDER_ID
        if series_name and not flat_upload:
            parent_folder = get_or_create_folder(service, series_name, DRIVE_FOLDER_ID)

        os.makedirs("downloads", exist_ok=True)
        for idx, f_info in enumerate(file_list, 1):
            if ACTIVE_TASKS[task_id]['cancelled']: break
            
            filename = f_info['name']
            clean_name = clean_filename(filename)
            path = f"downloads/{filename}"

            # Step 1: Download
            await status_msg.edit_text(f"📥 **Downloading ({idx}/{len(file_list)})**\n`{filename}`")
            await client.download_media(message=await client.get_messages(status_msg.chat.id, f_info['msg_id']), file_name=path)

            # Step 2: Folder Logic
            target_folder = parent_folder
            if not flat_upload:
                target_folder = get_or_create_folder(service, os.path.splitext(clean_name)[0], parent_folder)

            # Step 3: Resumable Upload
            media = MediaFileUpload(path, resumable=True, chunksize=5*1024*1024)
            request = service.files().create(body={'name': clean_name, 'parents': [target_folder]}, media_body=media, fields='id, size')
            
            response = None
            while response is None:
                if ACTIVE_TASKS[task_id]['cancelled']: break
                status, response = request.next_chunk()
                if status:
                    prog = int(status.progress() * 100)
                    if prog % 20 == 0:
                        try: await status_msg.edit_text(f"☁️ **Uploading ({idx}/{len(file_list)})**\n`{clean_name}`\nProgress: {prog}%")
                        except: pass

            TOTAL_FILES += 1
            TOTAL_BYTES += int(response.get('size', 0))
            if os.path.exists(path): os.remove(path)
            sync_stats(service, save=True)

        await status_msg.edit_text(f"✅ **Complete!**\nFiles: {len(file_list)}\nTotal: {format_size(TOTAL_BYTES)}")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {e}")
    finally:
        ACTIVE_TASKS.pop(task_id, None)

# ==================== DRIVE BROWSER (DOWNLOAD MODE) ====================

async def get_browser_menu(service, fid='root', pname="Root"):
    try:
        q = "'" + fid + "' in parents and trashed=false"
        res = service.files().list(q=q, fields="files(id, name, mimeType, size)", orderBy="folder, name").execute().get('files', [])
        
        btns = []
        for f in res:
            if f['mimeType'] == 'application/vnd.google-apps.folder':
                btns.append([InlineKeyboardButton("📁 " + f['name'], callback_data="brw|" + f['id'])])
            else:
                btns.append([InlineKeyboardButton("📄 " + f['name'] + " (" + format_size(int(f.get('size', 0))) + ")", callback_data="mir|" + f['id'])])
        
        # Back logic
        pid = 'root'
        if fid != 'root':
            p_res = service.files().get(fileId=fid, fields='parents').execute().get('parents', [])
            pid = p_res[0] if p_res else 'root'
            
        btns.append([InlineKeyboardButton("⬅️ Back", callback_data="brw|" + pid), InlineKeyboardButton("🏠 Menu", callback_data="back_start")])
        return "📍 **Path:** `" + pname + "`", InlineKeyboardMarkup(btns)
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

        if name.lower().endswith(('.mp3', '.m4b')):
            await client.send_audio(query.message.chat.id, audio=path, caption=f"✅ `{name}`")
        else:
            await client.send_document(query.message.chat.id, document=path, caption=f"✅ `{name}`")
        
        await msg.delete()
        if os.path.exists(path): os.remove(path)
    except Exception as e: await query.message.edit_text(f"❌ Mirror Error: {e}")

# ==================== HANDLERS ====================

@app.on_message(filters.command("start") & filters.user(OWNER_ID))
async def start_cmd(client, message):
    btns = [[InlineKeyboardButton("📤 Upload Mode", callback_data="m_up")], [InlineKeyboardButton("📥 Download Mode", callback_data="m_down")]]
    await message.reply_text("🤖 **RxUploader Command Center**\nSelect a mode to begin:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex("back_start"))
async def back_start(client, query):
    await start_cmd(client, query.message)

@app.on_callback_query(filters.regex("m_down"))
async def m_down(client, query):
    s = get_drive_service()
    t, m = await get_browser_menu(s)
    await query.message.edit_text(t, reply_markup=m)

@app.on_callback_query(filters.regex(r"^brw\|"))
async def brw_cb(client, query):
    fid = query.data.split("|")[1]
    s = get_drive_service()
    pn = "Folder"
    try: pn = s.files().get(fileId=fid, fields='name').execute()['name']
    except: pass
    t, m = await get_browser_menu(s, fid, pn)
    await query.message.edit_text(t, reply_markup=m)

@app.on_callback_query(filters.regex(r"^mir\|"))
async def mir_cb(client, query):
    await mirror_file(client, query, query.data.split("|")[1])

@app.on_message(filters.media & filters.user(OWNER_ID))
async def media_handler(client, message):
    # Restore Album Logic
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
                f_list = ALBUMS.pop(gid)
                key = f"alb_{gid}"
                TEMP_FILES[key] = f_list
                det_cap = next((f['caption'] for f in f_list if f['caption']), None)
                cl_name = clean_series_name(f_list[0]['name'])
                
                btns = []
                if det_cap: btns.append([InlineKeyboardButton(f"📂 Series: {det_cap[:25]}", callback_data=f"cap|{key}")])
                else: btns.append([InlineKeyboardButton(f"📂 Series: {cl_name[:25]}", callback_data=f"auto|{key}")])
                btns.append([InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")])
                btns.append([InlineKeyboardButton("🚫 Not Audiobook", callback_data=f"root|{key}")])
                btns.append([InlineKeyboardButton("✏️ Custom Name", callback_data=f"cust|{key}")])
                await message.reply_text(f"📦 **Album Detected** ({len(f_list)} files)", reply_markup=InlineKeyboardMarkup(btns))
            asyncio.create_task(process_album())
        ALBUMS[gid].append(info)
    else:
        key = f"sig_{message.id}"
        TEMP_FILES[key] = [info]
        cl_name = clean_series_name(fname)
        btns = []
        if cap: btns.append([InlineKeyboardButton(f"📂 Series: {cap[:25]}", callback_data=f"cap|{key}")])
        else: btns.append([InlineKeyboardButton(f"📂 Series: {cl_name[:25]}", callback_data=f"auto|{key}")])
        btns.append([InlineKeyboardButton("📁 Standalone", callback_data=f"std|{key}")])
        btns.append([InlineKeyboardButton("✏️ Custom Name", callback_data=f"cust|{key}")])
        btns.append([InlineKeyboardButton("🚫 Not Audiobook", callback_data=f"root|{key}")])
        await message.reply_text(f"📄 **File:** `{fname}`", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^(cap|auto|std|root|cust)\|"))
async def up_callback(client, query):
    mode, key = query.data.split("|")
    files = TEMP_FILES.get(key)
    if not files: return await query.answer("Session Expired.")

    if mode == "cust":
        ACTIVE_SERIES[query.from_user.id] = {'files': files, 'key': key}
        return await query.message.edit_text("✏️ Reply with the **Custom Series Name**:")

    series = None
    flat = False
    if mode == "cap": series = next((f['caption'] for f in files if f['caption']), "Unknown")
    elif mode == "auto": series = clean_series_name(files[0]['name'])
    elif mode == "root": flat = True

    await query.message.edit_text("🚀 Starting upload task...")
    asyncio.create_task(upload_task(client, query.message, files, series, flat))
    TEMP_FILES.pop(key, None)

@app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start"]))
async def custom_name_handler(client, message):
    if message.from_user.id in ACTIVE_SERIES:
        data = ACTIVE_SERIES.pop(message.from_user.id)
        status = await message.reply_text(f"🚀 Series Upload: **{message.text}**")
        asyncio.create_task(upload_task(client, status, data['files'], series_name=message.text))

if __name__ == "__main__":
    service = get_drive_service()
    if service: sync_stats(service, save=False)
    app.run()
