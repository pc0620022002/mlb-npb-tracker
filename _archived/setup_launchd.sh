#!/bin/bash
# Setup launchd to replace cron for baseball notifier
# This ensures the bot runs even after Mac wakes from sleep

BOT_DIR="$HOME/baseball_bot"
PLIST_NAME="com.andy.baseball-notifier"
PLIST_PATH="$HOME/Library/LaunchAgents/${PLIST_NAME}.plist"

echo "=== Setting up launchd for Baseball Bot ==="

# Remove old cron job if exists
(crontab -l 2>/dev/null | grep -v baseball_notifier) | crontab - 2>/dev/null || true
echo "✅ Removed old cron job (if any)"

# Unload old launchd job if exists
launchctl unload "$PLIST_PATH" 2>/dev/null || true

# Create launchd plist
mkdir -p "$HOME/Library/LaunchAgents"
cat > "$PLIST_PATH" << PLISTEOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PLIST_NAME}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>${BOT_DIR}/baseball_notifier.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${BOT_DIR}</string>
    <key>StartInterval</key>
    <integer>300</integer>
    <key>StandardOutPath</key>
    <string>${BOT_DIR}/launchd.log</string>
    <key>StandardErrorPath</key>
    <string>${BOT_DIR}/launchd.log</string>
    <key>RunAtLoad</key>
    <true/>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/bin:/usr/local/bin:/opt/homebrew/bin</string>
    </dict>
</dict>
</plist>
PLISTEOF

echo "✅ Created plist: $PLIST_PATH"

# Load the launchd job
launchctl load "$PLIST_PATH"
echo "✅ Loaded launchd job (runs every 5 minutes)"

echo ""
echo "=== Setup Complete ==="
echo "The bot runs every 5 mins, even after Mac wakes from sleep."
echo "To stop:  launchctl unload $PLIST_PATH"
echo "To start: launchctl load $PLIST_PATH"
echo "To check: launchctl list | grep baseball"
