# RxUploader

Telegram bot. Google Drive integration. No bullshit.

## What It Does

**Upload**: Send files to Telegram. Bot dumps them to Drive.  
**Download**: Paste Drive link. Bot sends files back.  
**Browse**: Navigate your Drive from Telegram.

That's it.

## Features

- Real-time progress bars (speed, ETA, all that)
- Queue system (upload multiple files, bot handles it)
- Series detection (sends "Breaking.Bad.S01E01.mkv" â†’ creates "Breaking Bad" folder)
- File filtering (only uploads audio/video, ignores junk)
- Cancel anytime (button or `/cancel`)
- Auto-retry on failure (3 attempts max)
- Filename cleaning (replaces underscores with spaces)

## Requirements

```
Python 3.8+
Telegram API credentials
Google Drive API access
```

## Install

```bash
git clone https://github.com/rollins1338/telegram-drive-bot-2.git
cd telegram-drive-bot-2
pip install -r requirements.txt
```

## Configure

Create `.env`:

```env
API_ID=your_api_id
API_HASH=your_api_hash
TELEGRAM_TOKEN=bot_token_from_botfather
OWNER_ID=your_telegram_user_id
DRIVE_FOLDER_ID=target_drive_folder
TOKEN_JSON='{"token":"...","refresh_token":"...","client_id":"...","client_secret":"...","scopes":["https://www.googleapis.com/auth/drive.file"]}'
```

**Get credentials:**
- Telegram: [my.telegram.org](https://my.telegram.org)
- Bot token: [@BotFather](https://t.me/botfather)
- User ID: [@userinfobot](https://t.me/userinfobot)
- Drive: [Google Cloud Console](https://console.cloud.google.com/) â†’ Enable Drive API â†’ OAuth credentials

## Run

```bash
python bot.py
```

Health check runs on port 8000.

## Commands

```
/start      - Wake up
/stats      - Numbers
/queue      - What's waiting
/browse     - Navigate Drive
/search     - Find files
/cancel     - Stop everything
/retry      - Retry failures
```

## How It Works

**Upload Flow:**
1. Send file to bot
2. Bot asks: Series? Standalone? Root?
3. Downloads from Telegram
4. Uploads to Drive
5. Done

**Download Flow:**
1. Paste Drive link
2. Bot shows files/folders
3. Select what you want
4. Bot downloads from Drive
5. Sends to Telegram
6. Done

**Progress Tracking:**
- Shows download speed (Telegram â†’ Bot)
- Shows upload speed (Bot â†’ Drive)
- Real-time ETA calculations
- Cancel button on every operation

## File Handling

Bot only processes **audio and video files**. Everything else gets skipped.

Supported formats:
- Audio: mp3, m4a, m4b, flac, wav, aac, ogg, opus
- Video: mp4, mkv, avi, mov, wmv, flv, webm

## Deploy

**Local:**
```bash
python bot.py
```

**Docker:**
```bash
docker build -t rxuploader .
docker run -d --env-file .env rxuploader
```

**Koyeb/VPS:**
- Set environment variables
- Point to GitHub repo
- Deploy
- Health check: `http://your-host:8000`

## Security

- Owner ID verification on all commands
- No public access
- Environment variables only
- Never commit: `.env`, `token.json`, `*.session`

## Troubleshooting

**Bot doesn't respond:**
- Check `TELEGRAM_TOKEN`
- Verify `OWNER_ID` is correct

**Drive errors:**
- Check `TOKEN_JSON` scopes
- Enable Drive API in Cloud Console
- Verify folder permissions

**Upload fails:**
- Check Drive storage quota
- Verify file size limits

## Tech Stack

- Pyrogram (Telegram API)
- Google Drive API v3
- Python 3.8+

## License

MIT. Do whatever.

## Notes

- Bot is single-user (owner only)
- Health check server runs on port 8000
- Files filtered: only audio/video uploaded
- Queue system handles concurrent uploads
- Auto-retry on failures (max 3 attempts)
- Progress updates every 2 seconds (avoids flood limits)

---

