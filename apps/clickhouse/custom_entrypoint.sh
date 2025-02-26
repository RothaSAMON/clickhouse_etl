#!/bin/bash

MIGRATIONS_DIR=/migrations
APPLIED_MIGRATIONS_FILE=/var/lib/applied_migrations.txt

# Start the original entrypoint script in the background
echo "Starting ClickHouse server..."
/entrypoint.sh "$@" &
ENTRYPOINT_PID=$!

echo "Waiting for ClickHouse to start..."
RETRY_COUNT=0
MAX_RETRIES=30
until clickhouse-client --query "SELECT 1" &>/dev/null; do
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ "$RETRY_COUNT" -ge "$MAX_RETRIES" ]; then
    echo "ClickHouse server did not become ready in time. Exiting."
    exit 1
  fi
  echo "ClickHouse is not ready yet. Retrying in 2 seconds..."
  sleep 2
done

# Create the database if it doesn't exist
echo "Ensuring database '${CLICKHOUSE_DB}' exists..."
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB};"

# Initialize the applied migrations log if it doesn't exist
if [ ! -f "$APPLIED_MIGRATIONS_FILE" ]; then
  echo "Initializing applied migrations log..."
  touch "$APPLIED_MIGRATIONS_FILE"
fi

# Run new migration files
echo "Checking for new migrations in ${MIGRATIONS_DIR}..."
for migration in ${MIGRATIONS_DIR}/*.sql; do
  if [ -f "$migration" ]; then
    # Get the migration file name
    migration_name=$(basename "$migration")

    # Check if the migration has already been applied
    if ! grep -Fxq "$migration_name" "$APPLIED_MIGRATIONS_FILE"; then
      echo "Applying migration: $migration_name"
      if clickhouse-client --database=${CLICKHOUSE_DB} --multiquery < "$migration"; then
        # Log the applied migration
        echo "$migration_name" >> "$APPLIED_MIGRATIONS_FILE"
        echo "Migration $migration_name applied successfully."
      else
        echo "Error applying migration $migration_name. Exiting."
        exit 1
      fi
    else
      echo "Migration $migration_name already applied. Skipping."
    fi
  fi
done

# Wait for the original entrypoint to finish
wait "$ENTRYPOINT_PID"
