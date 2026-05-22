#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
export PATH="$SCRIPT_DIR/venv/bin:$PATH"
python desktop_widget_cluster.py
