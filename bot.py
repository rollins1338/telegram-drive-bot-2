import os, json, asyncio, threading, time, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ForceReply
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Koyeb Health Check Hack
def run_health_server():
    HTTPServer(('0.0.0.0', 8000), type('H', (BaseHTTPRequestHandler,), {'do_GET': lambda s: (s.send_response(200), s.end_headers(), s.wfile.write(b"OK"))})).serve_forever()
threading.Thread(target=run_health_server, daemon=True).start()

# Config
ID, HASH, TOKEN = int(os.getenv('API_ID')), os.getenv('API_HASH'), os.getenv('TELEGRAM_TOKEN')
DRIVE_ID, OWNER = os.getenv('DRIVE_FOLDER_ID'), int(os.getenv('OWNER_ID', 0))
START_TIME, FILES, BYTES = time.time(), 0, 0
app = Client("bot", api_id=ID, api_hash=HASH, bot_token=TOKEN, ipv6=False)

def get_service():
    return build('drive', 'v3', credentials=Credentials.from_authorized_user_info(json.loads(os.getenv('TOKEN_JSON'))))

def get_folder(svc, name, parent):
    q = f"name='{name}' and '{parent}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res = svc.files().list(q=q, fields="files(id)").execute().get('files', [])
    if res: return res[0]['id']
    return svc.files().create(body={'name': name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent]}, fields='id').execute().get('id')

async def prog(current, total, msg, start, name, action):
    now = time.time()
    if not hasattr(msg, 'last') or (now - msg.last) > 5 or current == total:
        msg.last = now
        elap = now - start or 0.1
        speed = current / elap
        perc = current * 100 / total
        bar = '■' * int(perc // 10) + '□' * (10 - int(perc // 10))
        text = f"⏳ **{action}**\n`{name}`\n[{bar}] {perc:.1f}%\n⚡ {speed/1024/1024:.2f} MB/s"
        try: await msg.edit_text(text)
        except: pass

async def upload(client, status_msg, f_info, series=None):
    global FILES, BYTES
    svc, path, start = get_service(), f"downloads/{f_info['name']}", time.time()
    
    # Download
    await client.download_media(f_info['msg_id'], file_name=path, progress=prog, progress_args=(status_msg, start, f_info['name'], "Downloading"))
    
    # Upload
    await status_msg.edit_text("☁️ **Uploading...**")
    parent = get_folder(svc, series, DRIVE_ID) if series else DRIVE_ID
    book_dir = get_folder(svc, f_info['name'].rsplit('.', 1)[0], parent)
    
    file = svc.files().create(body={'name': f_info['name'], 'parents': [book_dir]}, 
                              media_body=MediaFileUpload(path, resumable=True), fields='size').execute()
    
    FILES += 1
    BYTES += int(file.get('size', 0))
    if os.path.exists(path): os.remove(path)
    await status_msg.edit_text(f"✅ Done: {series or 'Root'}")

@app.on_message(filters.command("stats") & filters.user(OWNER))
async def stats(c, m):
    up = str(datetime.timedelta(seconds=int(time.time() - START_TIME)))
    gb = BYTES / (1024**3)
    await m.reply(f"⏱ **Uptime:** `{up}`\n📂 **Files:** `{FILES}`\n📶 **Data:** `{gb:.2f} GB`")

@app.on_message(filters.media & filters.user(OWNER))
async def on_media(c, m):
    file = getattr(m, m.media.value)
    name = getattr(file, "file_name", "file")
    await m.reply(f"📄 {name}", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 Standalone", callback_data=f"std|{m.id}|{name}")],
        [InlineKeyboardButton("📂 Series", callback_data=f"ser|{m.id}|{name}")]
    ]))

@app.on_callback_query()
async def cb(c, q):
    mode, mid, name = q.data.split('|')
    f_info = {'msg_id': int(mid), 'name': name}
    if mode == "std":
        await q.message.edit_text("⏳ Starting...")
        asyncio.create_task(upload(c, q.message, f_info))
    else:
        await q.message.edit_text("✍️ Reply with Series Name", reply_markup=ForceReply(selective=True))
        app.temp = f_info

@app.on_message(filters.reply & filters.text & filters.user(OWNER))
async def on_reply(c, m):
    if hasattr(app, 'temp'):
        f_info, app.temp = app.temp, None
        status = await m.reply("⏳ Starting...")
        asyncio.create_task(upload(c, status, f_info, m.text.strip()))

app.run()
