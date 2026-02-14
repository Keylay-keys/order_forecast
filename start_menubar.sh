#!/bin/bash
# Start RouteSpark Menu Bar App
cd "$(dirname "$0")"
source venv/bin/activate
python menubar_app.py &
echo "RouteSpark Menu Bar App started"

