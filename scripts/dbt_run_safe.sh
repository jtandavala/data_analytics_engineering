#!/bin/bash

# Script to run dbt with reduced concurrency to avoid deadlocks
# Usage: ./scripts/dbt_run_safe.sh [dbt_command]
# Example: ./scripts/dbt_run_safe.sh "run --select fact_orders"

cd "$(dirname "$0")/../data_platform" || exit 1

# Default to 2 threads to reduce deadlock risk
# Adjust this number if you still encounter deadlocks (try 1 for sequential execution)
THREADS=${DBT_THREADS:-2}

# Maximum retries on deadlock
MAX_RETRIES=${DBT_MAX_RETRIES:-3}

# Get dbt command from arguments, default to "run"
DBT_CMD=${1:-run}

# Remove --threads if already present and use our safe thread count
CLEANED_CMD=$(echo "$DBT_CMD" | sed 's/--threads[[:space:]]*[0-9]*//g')

# Function to check if error is a deadlock
is_deadlock() {
    grep -q "deadlock detected" <<< "$1"
}

# Run dbt with reduced concurrency, retry on deadlock
RETRY_COUNT=0
while [ $RETRY_COUNT -le $MAX_RETRIES ]; do
    if [ $RETRY_COUNT -gt 0 ]; then
        echo "Retry attempt $RETRY_COUNT of $MAX_RETRIES after deadlock..."
        sleep 2  # Wait a bit before retrying
    else
        echo "Running dbt with $THREADS threads to avoid deadlocks..."
    fi
    
    OUTPUT=$(dbt $CLEANED_CMD --threads $THREADS 2>&1)
    EXIT_CODE=$?
    
    echo "$OUTPUT"
    
    # If successful, exit
    if [ $EXIT_CODE -eq 0 ]; then
        exit 0
    fi
    
    # If deadlock and retries left, try again
    if is_deadlock "$OUTPUT" && [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
        RETRY_COUNT=$((RETRY_COUNT + 1))
        continue
    fi
    
    # Otherwise, exit with error
    exit $EXIT_CODE
done
