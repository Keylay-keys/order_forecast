#!/bin/bash
# Order Archive Listener Startup Script
# Uses real-time Firestore on_snapshot (like PCF OCR) - instant response!
# Keeps the listener alive using caffeinate (prevents Mac sleep)
#
# Usage:
#   ./start_archive_listener.sh           # Run in foreground
#   ./start_archive_listener.sh &         # Run in background
#   nohup ./start_archive_listener.sh &   # Run detached (survives terminal close)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="$SCRIPT_DIR/venv"
DUCKDB_PATH="$SCRIPT_DIR/data/analytics.duckdb"
SA_PATH="/Users/kylemacmini/Desktop/dev/firebase-tools/routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json"

echo "🎧 Starting Order Archive Listener (Real-time)..."
echo "   Database: $DUCKDB_PATH"
echo ""

# Activate virtual environment
source "$VENV_PATH/bin/activate"

# Run with caffeinate to prevent sleep
# -d: prevent display sleep
# -i: prevent idle sleep  
# -m: prevent disk sleep
caffeinate -dim python "$SCRIPT_DIR/scripts/order_archive_listener.py" \
    --serviceAccount "$SA_PATH" \
    --duckdb "$DUCKDB_PATH"

