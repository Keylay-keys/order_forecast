#!/bin/bash
# Start the retrain daemon with caffeinate to keep Mac awake

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate virtual environment
source venv/bin/activate

# Default route
ROUTE="${1:-989262}"

echo "Starting retrain daemon for route $ROUTE..."
echo "Press Ctrl+C to stop"
echo ""

# Run with caffeinate to prevent sleep
# -i = prevent idle sleep
# -s = prevent system sleep (even when on battery)
caffeinate -is python scripts/retrain_daemon.py --route "$ROUTE" --interval 3600

