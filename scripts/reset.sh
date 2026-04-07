#!/bin/bash

echo "🔄 Resetting MN Fair Pipeline..."

# Ensure data directory exists
mkdir -p data

# Remove database
if [ -f data/fair.db ]; then
    rm data/fair.db
    echo "🗑️ Removed fair.db"
else
    echo "ℹ️ fair.db not found"
fi

# Remove staging file
if [ -f data/staging_transactions.csv ]; then
    rm data/staging_transactions.csv
    echo "🗑️ Removed staging_transactions.csv"
else
    echo "ℹ️ staging file not found"
fi

# Recreate staging file
touch data/staging_transactions.csv

# Remove last extracted tracker
if [ -f data/last_extracted_id.txt ]; then
    rm data/last_extracted_id.txt
    echo "🗑️ Removed last_extracted_id.txt"
else
    echo "ℹ️ last_extracted_id.txt not found"
fi

# Recreate tracker file
echo "0" > data/last_extracted_id.txt
echo "🔢 Reset last_extracted_id.txt to 0"

# Recreate database
python3 scripts/setup_db.py

echo "✅ Reset complete!"