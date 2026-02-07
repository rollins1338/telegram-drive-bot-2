# RxUploader Bot

Telegram bot for Google Drive integration. Upload and download files through Telegram with progress tracking and queue management.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## Features

**Upload**
- Send files → Auto-upload to Drive
- Series detection & folder organization
- Real-time progress with speed/ETA
- Queue management & retry support

**Download**
- Send Drive link → Get file in Telegram
- Single files & entire folders
- File browser with search
- Favorites system

## Quick Start

```bash
# Clone and install
git clone https://github.com/yourusername/rxuploader-bot.git
cd rxuploader-bot
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your credentials

# Run
python bot_ui_improved.py
```

## Environment Setup

Get credentials from:
- **Telegram:** [my.telegram.org](https://my.telegram.org) + [@BotFather](https://t.me/botfather)
- **Google Drive:** [Cloud Console](https://console.cloud.google.com/) (Enable Drive API, create OAuth credentials)

Required variables (see `.env.example`):
```
API_ID=
API_HASH=
TELEGRAM_TOKEN=
OWNER_ID=
DRIVE_FOLDER_ID=
TOKEN_JSON=
```

## Commands

| Command | Action |
|---------|--------|
| `/start` | Start bot |
| `/stats` | Statistics |
| `/queue` | Upload queue |
| `/browse` | Browse Drive |
| `/search <query>` | Search files |
| `/cancel` | Cancel uploads |
| `/retry` | Retry failed |

## Deployment

Compatible with Koyeb, Heroku, VPS. Health check on port 8000.

## Security

- Environment variables for all credentials
- Owner ID verification
- `.gitignore` protects secrets
- **Never commit:** `.env`, `token.json`, `*.session`, credential files

## License

MIT
