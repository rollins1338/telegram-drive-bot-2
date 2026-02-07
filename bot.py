#!/usr/bin/env python3
"""
Google Drive Permission Diagnostic Script
Run this to figure out what's wrong with your bot's permissions
"""

import os
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def main():
    print("=" * 60)
    print("🔍 GOOGLE DRIVE PERMISSION DIAGNOSTIC")
    print("=" * 60)
    print()

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
                pass  # Suppress health check logs
        
        HTTPServer(('0.0.0.0', 8000), HealthHandler).serve_forever()
    except Exception as e:
        logger.error(f"Health server error: {e}")

threading.Thread(target=run_health_server, daemon=True).start()
logger.info("🏥 Health check server starting on port 8000")
    
    # Step 1: Load TOKEN_JSON
    print("📋 Step 1: Loading TOKEN_JSON...")
    TOKEN_JSON = os.getenv('TOKEN_JSON')
    
    if not TOKEN_JSON:
        print("❌ ERROR: TOKEN_JSON environment variable not set!")
        print()
        print("Set it with:")
        print('export TOKEN_JSON=\'{"token":"...", "refresh_token":"..."}\'')
        return
    
    try:
        creds_data = json.loads(TOKEN_JSON)
        print("✅ TOKEN_JSON loaded successfully")
    except json.JSONDecodeError as e:
        print(f"❌ ERROR: Invalid TOKEN_JSON format: {e}")
        return
    
    print()
    
    # Step 2: Authenticate with Drive
    print("🔐 Step 2: Authenticating with Google Drive...")
    try:
        credentials = Credentials.from_authorized_user_info(creds_data)
        service = build('drive', 'v3', credentials=credentials)
        print("✅ Successfully authenticated")
    except Exception as e:
        print(f"❌ ERROR: Failed to authenticate: {e}")
        return
    
    print()
    
    # Step 3: Get bot's email
    print("📧 Step 3: Getting bot's email address...")
    try:
        about = service.about().get(fields="user").execute()
        bot_email = about['user']['emailAddress']
        print(f"✅ Bot Email: {bot_email}")
        print()
        print("⚠️  IMPORTANT: Share files with this email address!")
        print("   Give it Editor or Owner access in Google Drive")
    except Exception as e:
        print(f"❌ ERROR: Couldn't get bot email: {e}")
        bot_email = "Unknown"
    
    print()
    print("-" * 60)
    print()
    
    # Step 4: Test file access
    print("🧪 Step 4: Testing file access...")
    print()
    
    # Ask for file URL
    print("Please enter a Google Drive file or folder URL to test:")
    print("Example: https://drive.google.com/file/d/1ABC123.../view")
    print()
    url = input("URL: ").strip()
    
    if not url:
        print("❌ No URL provided, skipping file test")
        return
    
    # Extract file ID
    import re
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)',
        r'/folders/([a-zA-Z0-9_-]+)',
        r'/d/([a-zA-Z0-9_-]+)',
    ]
    
    file_id = None
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            file_id = match.group(1)
            break
    
    if not file_id:
        print("❌ ERROR: Couldn't extract file ID from URL")
        return
    
    print(f"📋 File ID: {file_id}")
    print()
    
    # Try to access the file
    print("🔍 Attempting to access file...")
    try:
        metadata = service.files().get(
            fileId=file_id,
            fields="id, name, mimeType, size, webViewLink, permissions, capabilities, owners",
            supportsAllDrives=True
        ).execute()
        
        print("✅ SUCCESS! File is accessible")
        print()
        print("📄 File Details:")
        print(f"   Name: {metadata.get('name')}")
        print(f"   Type: {metadata.get('mimeType')}")
        print(f"   Size: {metadata.get('size', 'N/A')} bytes")
        print()
        
        # Check owners
        owners = metadata.get('owners', [])
        if owners:
            print("👤 Owners:")
            for owner in owners:
                print(f"   - {owner.get('emailAddress', 'Unknown')}")
        print()
        
        # Check permissions
        permissions = metadata.get('permissions', [])
        if permissions:
            print("🔐 Permissions:")
            for perm in permissions:
                email = perm.get('emailAddress', 'Anyone' if perm.get('type') == 'anyone' else 'Unknown')
                role = perm.get('role', 'Unknown')
                perm_type = perm.get('type', 'Unknown')
                print(f"   - {email} ({perm_type}): {role}")
        else:
            print("🔐 Permissions: (Unable to retrieve)")
        print()
        
        # Check capabilities
        capabilities = metadata.get('capabilities', {})
        can_download = capabilities.get('canDownload', False)
        can_read = capabilities.get('canReadRevisions', False)
        
        print("⚡ Capabilities:")
        print(f"   Can Download: {'✅ Yes' if can_download else '❌ No'}")
        print(f"   Can Read: {'✅ Yes' if can_read else '❌ No'}")
        print()
        
        if not can_download:
            print("⚠️  WARNING: Bot cannot download this file!")
            print("   Make sure the bot has Editor access, not just Viewer")
        
        # Check if bot email has permission
        bot_has_access = False
        for perm in permissions:
            if perm.get('emailAddress') == bot_email:
                bot_has_access = True
                print(f"✅ Bot email ({bot_email}) has {perm.get('role')} access")
                break
        
        if not bot_has_access:
            print(f"⚠️  WARNING: Bot email ({bot_email}) not found in permissions!")
            print("   You need to share the file with this email")
        
        print()
        print("=" * 60)
        print("✅ DIAGNOSTIC COMPLETE")
        print("=" * 60)
        
    except HttpError as e:
        print(f"❌ ERROR: Failed to access file")
        print()
        print(f"HTTP Status: {e.resp.status}")
        print(f"Error: {e}")
        print()
        
        if e.resp.status == 404:
            print("❌ File not found or bot doesn't have access")
            print()
            print("Solutions:")
            print(f"1. Share the file with: {bot_email}")
            print("2. Give it Editor or Owner access")
            print("3. Make sure the file exists and isn't deleted")
            
        elif e.resp.status == 403:
            print("❌ Permission denied")
            print()
            print("Solutions:")
            print(f"1. Share the file with: {bot_email}")
            print("2. Give it Editor or Owner access (NOT just Viewer)")
            print("3. Check if the file is restricted")
            
        else:
            print("Solutions:")
            print(f"1. Share the file with: {bot_email}")
            print("2. Wait 1-2 minutes after sharing")
            print("3. Try again")
        
        print()
        print("=" * 60)
        print("❌ DIAGNOSTIC FAILED")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ ERROR: Unexpected error: {e}")
        print()
        print("=" * 60)
        print("❌ DIAGNOSTIC FAILED")
        print("=" * 60)

if __name__ == "__main__":
    main()
