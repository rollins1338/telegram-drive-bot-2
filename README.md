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

Built for personal use. Don't abuse it.3. Select what you want
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

Built for personal use. Don't abuse it.2. Create a new project or select existing
3. Enable Google Drive API
4. Create OAuth 2.0 credentials (Desktop app)
5. Download credentials and generate `TOKEN_JSON` with required scopes:
   - `https://www.googleapis.com/auth/drive`

See [Authentication Guide](docs/AUTHENTICATION.md) for detailed steps.

## Commands

| Command | Description |
|---------|-------------|
| `/start` | Initialize bot and show welcome message |
| `/stats` | Display upload statistics and bot status |
| `/queue` | View current upload queue |
| `/browse` | Open interactive file browser |
| `/search <query>` | Search for files in Drive |
| `/cancel` | Cancel active upload tasks |
| `/retry` | Retry failed uploads |
| `/clearfailed` | Clear failed upload history |

## File Browser

The interactive file browser provides:
- Navigate through Drive folders
- Select multiple files/folders
- Preview file information
- Upload selected items to Telegram
- Search within current folder
- Bookmark favorite locations

## Deployment

### Local Development
```bash
python bot.py
```

### Production Deployment

#### Koyeb
1. Fork this repository
2. Connect to Koyeb
3. Set environment variables in Koyeb dashboard
4. Deploy

#### Docker
```bash
docker build -t rxuploader-bot .
docker run -d --env-file .env rxuploader-bot
```

#### VPS
```bash
# Install dependencies
pip install -r requirements.txt

# Run with systemd or supervisor
# Health check endpoint: http://localhost:8000
```

The bot includes a health check server on port 8000 for monitoring.

## Architecture

- **Health Check Server**: HTTP server on port 8000 for uptime monitoring
- **Queue System**: Manages upload tasks with priority and retry logic
- **Session Management**: Maintains browser state per user
- **Error Recovery**: Automatic retry with exponential backoff
- **Progress Tracking**: Real-time callbacks for upload/download progress

## Security

- All sensitive credentials stored in environment variables
- Owner ID verification on all commands
- No credentials in source code or logs
- Supports private Drive folders

**Important**: Never commit these files:
- `.env`
- `token.json`
- `*.session`
- Any credential files

## Troubleshooting

### Common Issues

**Bot doesn't respond**
- Verify `TELEGRAM_TOKEN` is correct
- Check `OWNER_ID` matches your Telegram user ID

**Drive access denied**
- Ensure `TOKEN_JSON` has correct scopes
- Verify service account has access to folders
- Check Drive API is enabled in Cloud Console

**Upload failures**
- Check Drive storage quota
- Verify folder permissions
- Review logs for specific errors


## Requirements

```
pyrogram>=2.0.0
tgcrypto>=1.2.0
google-api-python-client>=2.0.0
google-auth>=2.0.0
google-auth-oauthlib>=0.5.0
google-auth-httplib2>=0.1.0
```

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License.

## Acknowledgments

Built with:
- [Pyrogram](https://github.com/pyrogram/pyrogram) - Telegram MTProto API framework
- [Google Drive API](https://developers.google.com/drive) - Drive integration
- [Python](https://www.python.org/) - Core language

## Support

For issues and questions:
- Open an [issue](https://github.com/rollins1338/telegram-drive-bot/issues)

---

**Note**: This bot is for personal use. Ensure compliance with Google Drive and Telegram Terms of Service.- **Never commit:** `.env`, `token.json`, `*.session`, credential files
