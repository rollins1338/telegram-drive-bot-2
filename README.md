# RxUploader Bot

A Telegram bot for seamless Google Drive integration with advanced file management, progress tracking, and queue system.

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Features

### Upload to Drive
- Automatic file upload from Telegram to Google Drive
- Intelligent series detection and folder organization
- Real-time progress tracking with speed and ETA
- Queue management with priority and retry support
- Batch upload with album handling

### Download from Drive
- Send Drive links to download files directly to Telegram
- Support for single files and entire folder downloads
- Interactive file browser with pagination
- Search functionality and favorites system
- Multi-select file operations

### Progress Tracking
- Visual progress bars for all operations
- Real-time upload/download speed monitoring
- Accurate ETA calculations
- Two-phase tracking (Drive to Bot, Bot to Telegram)

## Installation

### Prerequisites
- Python 3.8 or higher
- Google Cloud Project with Drive API enabled
- Telegram Bot Token

### Setup

1. Clone the repository
```bash
git clone https://github.com/rollins1338/telegram-drive-bot-2.git
cd rxuploader-bot
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Configure environment variables
```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```env
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
TELEGRAM_TOKEN=your_bot_token
OWNER_ID=your_telegram_user_id
DRIVE_FOLDER_ID=your_drive_folder_id
TOKEN_JSON='{"token": "...", "refresh_token": "...", ...}'
```

4. Run the bot
```bash
python bot.py
```

## Getting Credentials

### Telegram Credentials
1. Get `API_ID` and `API_HASH` from [my.telegram.org](https://my.telegram.org)
2. Create a bot via [@BotFather](https://t.me/botfather) to get `TELEGRAM_TOKEN`
3. Get your `OWNER_ID` from [@userinfobot](https://t.me/userinfobot)

### Google Drive Credentials
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing
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
