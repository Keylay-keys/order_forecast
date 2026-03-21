#!/bin/bash
# PCF Archive Sync (cluster target)
# This is a sibling to the current Dell-targeted job.
# It is intentionally not installed or loaded by default.
#
# Install:
#   cp com.routespark.pcf-archive-sync-cluster.plist ~/Library/LaunchAgents/
#   launchctl load ~/Library/LaunchAgents/com.routespark.pcf-archive-sync-cluster.plist
#
# Uninstall:
#   launchctl unload ~/Library/LaunchAgents/com.routespark.pcf-archive-sync-cluster.plist

set -e

LOG_FILE="/Users/kylemacmini/Library/Logs/routespark-pcf-archive-sync-cluster.log"
PCF_DIR="/Users/kylemacmini/projects/pcf_pipeline"
PCF_VENV_PYTHON="$PCF_DIR/.venv/bin/python"

echo "$(date): Starting PCF archive sync (cluster)" >> "$LOG_FILE"

if ssh -o ConnectTimeout=3 -o BatchMode=yes keylay@192.168.1.40 true >/dev/null 2>&1; then
  export PCF_ARCHIVE_REMOTE="keylay@192.168.1.40:/mnt/archive/pcf/pcf_archive"
  echo "$(date): Using LAN archive target 192.168.1.40" >> "$LOG_FILE"
else
  export PCF_ARCHIVE_REMOTE="keylay@100.72.199.115:/mnt/archive/pcf/pcf_archive"
  echo "$(date): Falling back to Tailscale archive target 100.72.199.115" >> "$LOG_FILE"
fi

cd "$PCF_DIR"
"$PCF_VENV_PYTHON" scripts/sync_archive_to_server.py --delete-local >> "$LOG_FILE" 2>&1

echo "$(date): PCF archive sync complete (cluster)" >> "$LOG_FILE"
