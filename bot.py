import os
import logging
import json
import asyncio
import time
import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ForceReply
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- FAKE WEB SERVER FOR KOYEB HEALTH CHECKS ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"I am alive!")

def run_health_server():
    server = HTTPServer(('0.0.0.0', 8000), HealthCheckHandler)
    server.serve_forever()

# --- CONFIGURATION ---
OWNER_ID = int(os.environ.get('OWNER_ID', '0'))
API_ID = int(os.environ.get('API_ID', '0')) 
API_HASH = os.environ.get('API_HASH')
BOT_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TOKEN_JSON = os.environ.get('TOKEN_JSON')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID')

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- GLOBAL STORAGE ---
PENDING_FILES = {} 
FILES_UPLOADED = 0
TOTAL_BYTES = 0
BOT_START_TIME = time.time()

# --- HELPERS ---
def human_readable_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0: return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def get_readable_time(seconds):
    return str(datetime.timedelta(seconds=int(seconds)))

def clean_name(name):
    base = os.path.splitext(name)[0]
    return "".join([c for c in base if c.isalnum() or c in "._- "]).strip().replace(" ", ".")

def get_progress_bar_string(current, total):
    filled_length = int(10 * current // total)
    return '■' * filled_length + '□' * (10 - filled_length)

# --- GOOGLE DRIVE HELPERS ---
def get_drive_service():
    try:
        token_info = json.loads(TOKEN_JSON)
        creds = Credentials.from_authorized_user_info(token_info)
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        logger.error(f"Auth Error: {e}")
        return None

def create_or_get_folder(service, folder_name, parent_id):
    try:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and '{parent_id}' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        if files: return files[0]['id']
        
        metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = service.files().create(body=metadata, fields='id').execute()
        return folder.get('id')
    except Exception as e:
        logger.error(f"Folder Error: {e}")
        return None

def upload_to_drive_logic(file_path, file_name, mime_type, series_name=None):
    service = get_drive_service()
    if not service: return None, "Auth Failed"

    try:
        current_parent_id = DRIVE_FOLDER_ID
        if series_name:
            series_id = create_or_get_folder(service, series_name, DRIVE_FOLDER_ID)
            if not series_id: return None, "Failed to create Series Folder"
            current_parent_id = series_id

        book_folder_name = clean_name(file_name)
        book_id = create_or_get_folder(service, book_folder_name, current_parent_id)
        if not book_id: return None, "Failed to create Book Folder"

        clean_filename = f"{book_folder_name}{os.path.splitext(file_name)[1]}"
        file_metadata = {'name': clean_filename, 'parents': [book_id]}
        
        media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink, size'
        ).execute()
        return file, book_folder_name
    except Exception as e:
        logger.error(f"Upload Error: {e}")
        return None, str(e)

# --- WORKFLOW HANDLERS ---
async def progress_callback(current, total, message, start_time, file_name, action="Downloading"):
    now = time.time()
    if (now - getattr(message, "last_update_time", 0)) > 5 or current == total:
        elapsed_time = now - start_time
        if elapsed_time == 0: elapsed_time = 0.1
        speed = current / elapsed_time
        percentage = current * 100 / total
        eta = (total - current) / speed if speed > 0 else 0
        eta_str = time.strftime("%M:%S", time.gmtime(eta))
        
        text = (
            f"⏳ **{action}...**\n"
            f"📄 `{file_name}`\n"
            f"[{get_progress_bar_string(current, total)}] {percentage:.1f}%\n"
            f"⚡ {human_readable_size(speed)}/s | ⏱ ETA: {eta_str}"
        )
        try:
            await message.edit_text(text)
            message.last_update_time = now
        except:
            pass

async def process_workflow(client, status_msg, file_info, series_name=None):
    global FILES_UPLOADED, TOTAL_BYTES
    
    file_name = file_info['name']
    local_path = f"downloads/{file_name}"
    
    start_time = time.time()
    try:
        await status_msg.edit_text(f"📥 **Downloading...**\n`{file_name}`")
        original_msg = await client.get_messages(file_info['chat_id'], file_info['msg_id'])
        
        await original_msg.download(
            file_name=local_path,
            progress=progress_callback,
            progress_args=(status_msg, start_time, file_name, "Downloading")
        )
    except Exception as e:
        await status_msg.edit_text(f"❌ Download Failed: {e}")
        return

    await status_msg.edit_text(f"☁️ **Uploading to Drive...**\n`{file_name}`")
    loop = asyncio.get_running_loop()
    result, result_name = await loop.run_in_executor(
        None, 
        upload_to_drive_logic, 
        local_path, 
        file_name, 
        file_info['mime'], 
        series_name
    )

    if result:
        size = int(result.get('size', 0))
        FILES_UPLOADED += 1
        TOTAL_BYTES += size
        path_str = f"Root/{result_name}" if not series_name else f"{series_name}/{result_name}"
        
        await status_msg.edit_text(
            f"✅ **Done.**\n"
            f"📂 Path: `{path_str}`\n"
            f"🔗 [Open Drive Link]({result.get('webViewLink')})",
            disable_web_page_preview=True
        )
    else:
        await status_msg.edit_text(f"❌ Upload Error: {result_name}")

    if os.path.exists(local_path): os.remove(local_path)

# --- BOT SETUP ---
app = Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, ipv6=False)

def is_authorized(func):
    async def wrapper(client, message):
        if OWNER_ID != 0 and message.from_user.id != OWNER_ID: return
        await func(client, message)
    return wrapper

@app.on_message(filters.command("start"))
@is_authorized
async def start(client, message):
    await message.reply_text(
        "👋 **Bot is Online!**\n\n"
        "Send me any file and I will upload it to your Google Drive.\n"
        "Use /help to see available commands."
    )

@app.on_message(filters.command("help"))
@is_authorized
async def help_command(client, message):
    await message.reply_text(
        "📖 **Bot Help Menu**\n\n"
        "**Commands:**\n"
        "• /start - Restart the bot\n"
        "• /stats - Check session statistics\n"
        "• /help - Show this message\n\n"
        "**How to Upload:**\n"
        "1. Send a file to the bot.\n"
        "2. Select **Standalone** for a single book folder.\n"
        "3. Select **Add to Series** to group multiple books in a series folder."
    )

@app.on_message(filters.command("stats"))
@is_authorized
async def stats_command(client, message):
    uptime = time.time() - BOT_START_TIME
    uptime_str = get_readable_time(uptime)
    total_data = human_readable_size(TOTAL_BYTES)
    await message.reply_text(
        f"📊 **Session Statistics**\n\n"
        f"⏱ **Uptime:** `{uptime_str}`\n"
        f"📂 **Files:** `{FILES_UPLOADED}`\n"
        f"📶 **Data Moved:** `{total_data}`"
    )

@app.on_message(filters.media)
@is_authorized
async def handle_media(client, message):
    media = getattr(message, message.media.value)
    file_name = getattr(media, "file_name", None) or f"{message.media.value}_{message.id}"
    
    msg = await message.reply_text(
        f"📄 **File Detected:** `{file_name}`\n\nWhere should this go?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📚 Standalone", callback_data=f"mode_std|{message.id}")],
            [InlineKeyboardButton("📂 Add to Series", callback_data=f"mode_ser|{message.id}")]
        ])
    )
    
    PENDING_FILES[message.id] = {
        "chat_id": message.chat.id,
        "msg_id": message.id,
        "name": file_name,
        "mime": getattr(media, "mime_type", "application/octet-stream"),
        "status_msg_id": msg.id
    }

@app.on_callback_query()
async def callback_handler(client, callback):
    data = callback.data.split("|")
    mode = data[0]
    original_msg_id = int(data[1])
    
    if original_msg_id not in PENDING_FILES:
        await callback.answer("❌ Request expired.", show_alert=True)
        return

    file_info = PENDING_FILES[original_msg_id]
    
    if mode == "mode_std":
        await callback.message.edit_text(f"⏳ **Starting Standalone Process...**")
        asyncio.create_task(process_workflow(client, callback.message, file_info, None))
        del PENDING_FILES[original_msg_id]
        
    elif mode == "mode_ser":
        await callback.message.delete()
        prompt = await client.send_message(
            callback.message.chat.id, 
            f"✍️ **Enter Series Name** for:\n`{file_info['name']}`",
            reply_markup=ForceReply(selective=True)
        )
        PENDING_FILES[f"wait_series_{prompt.id}"] = file_info
        del PENDING_FILES[original_msg_id]

@app.on_message(filters.reply & filters.text)
@is_authorized
async def series_name_handler(client, message):
    reply_key = f"wait_series_{message.reply_to_message.id}"
    
    if reply_key in PENDING_FILES:
        file_info = PENDING_FILES[reply_key]
        series_name = message.text.strip()
        status_msg = await message.reply_text(f"⏳ **Starting Series Process: {series_name}...**")
        asyncio.create_task(process_workflow(client, status_msg, file_info, series_name))
        del PENDING_FILES[reply_key]

if __name__ == '__main__':
    if not os.path.exists("downloads"): os.makedirs("downloads")
    
    # Start the fake web server in a separate thread
    threading.Thread(target=run_health_server, daemon=True).start()
    
    # Start the Telegram bot
    app.run()
