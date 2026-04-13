#!/bin/bash

echo "🔄 Resetting MN Fair Pipeline..."

# ─────────────────────────────
# Ensure we run from project root
# ─────────────────────────────
cd "$(dirname "$0")/.." || exit 1

echo "📁 Working directory: $(pwd)"

# ─────────────────────────────
# Define paths
# ─────────────────────────────
DB_PATH="data/db/fair.db"
STAGING_FILE="data/staging/staging_transactions.csv"
STATE_FILE="data/state/last_extracted_id.txt"

# ─────────────────────────────
# Ensure directories exist
# ─────────────────────────────
mkdir -p data/db data/staging data/state

# ─────────────────────────────
# Reset database
# ─────────────────────────────
if [ -f "$DB_PATH" ]; then
    rm -f "$DB_PATH"
    echo "🗑️ Removed database"
else
    echo "ℹ️ Database not found"
fi

# ─────────────────────────────
# Reset staging file
# ─────────────────────────────
if [ -f "$STAGING_FILE" ]; then
    rm -f "$STAGING_FILE"
    echo "🗑️ Removed staging file"
else
    echo "ℹ️ Staging file not found"
fi

touch "$STAGING_FILE"

# ─────────────────────────────
# Reset state tracker
# ─────────────────────────────
if [ -f "$STATE_FILE" ]; then
    rm -f "$STATE_FILE"
    echo "🗑️ Removed state tracker"
else
    echo "ℹ️ State tracker not found"
fi

echo "0" > "$STATE_FILE"
echo "🔢 Reset last_extracted_id.txt to 0"

# ─────────────────────────────
# Recreate database schema
# ─────────────────────────────
python3 scripts/setup_db.py

echo "✅ Reset complete!"