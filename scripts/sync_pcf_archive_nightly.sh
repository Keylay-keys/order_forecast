#!/bin/bash
# PCF Archive Sync
# Run via LaunchAgent (configured for every 48 hours) to sync all archives to server and delete local copies
#
# Install:
#   cp com.routespark.pcf-archive-sync.plist ~/Library/LaunchAgents/
#   launchctl load ~/Library/LaunchAgents/com.routespark.pcf-archive-sync.plist
#
# Uninstall:
#   launchctl unload ~/Library/LaunchAgents/com.routespark.pcf-archive-sync.plist

set -e

LOG_FILE="/Users/kylemacmini/Library/Logs/routespark-pcf-archive-sync.log"
PCF_DIR="/Users/kylemacmini/projects/pcf_pipeline"
PCF_VENV_PYTHON="$PCF_DIR/.venv/bin/python"

echo "$(date): Starting PCF archive sync" >> "$LOG_FILE"

export PCF_ARCHIVE_REMOTE="keylay@100.64.201.120:/mnt/archive/pcf/pcf_archive"

# Run the sync script
cd "$PCF_DIR"
"$PCF_VENV_PYTHON" scripts/sync_archive_to_server.py --delete-local >> "$LOG_FILE" 2>&1

echo "$(date): PCF archive sync complete" >> "$LOG_FILE"
