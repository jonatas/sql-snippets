#!/bin/bash

# Load environment variables
if [ -f .env ]; then
    source .env
fi

# Check if a SQL file was provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <sql_file>"
    echo "Example: $0 sensors.sql"
    exit 1
fi

SQL_FILE=$1

# Check if the SQL file exists
if [ ! -f "$SQL_FILE" ]; then
    echo "Error: SQL file '$SQL_FILE' not found"
    exit 1
fi

echo "🐘 Running SQL snippet: $SQL_FILE"
echo "📍 Using TimescaleDB connection: $PGUSER@$PGHOST:$PGPORT/$PGDATABASE"
echo

# Run the SQL file with psql
PGPASSWORD=$PGPASSWORD psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --no-pager -f "$SQL_FILE"

echo
echo "✅ Finished executing $SQL_FILE"
