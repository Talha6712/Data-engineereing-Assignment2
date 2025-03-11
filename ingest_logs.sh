#!/bin/bash

# Check if date is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 YYYY-MM-DD"
    exit 1
fi

# Parse input date
DATE=$1
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# Set source and destination paths
SOURCE_DIR="/mnt/c/Users/talha/Downloads/raw_data/$DATE"
LOGS_HDFS_PATH="/raw/logs/$YEAR/$MONTH/$DAY"
METADATA_HDFS_PATH="/raw/metadata/$YEAR/$MONTH/$DAY"

# Check if source directory exists
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory $SOURCE_DIR does not exist"
    exit 1
fi

# Create HDFS directories if they don't exist
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p $LOGS_HDFS_PATH
hdfs dfs -mkdir -p $METADATA_HDFS_PATH

# Copy log files to HDFS
echo "Copying log files to HDFS: $LOGS_HDFS_PATH"
hdfs dfs -put -f $SOURCE_DIR/user_logs_*.csv $LOGS_HDFS_PATH/

# Copy metadata file to HDFS (if it exists for this date)
METADATA_FILE="/mnt/c/Users/talha/Downloads/raw_data/content_metadata.csv"
if [ -f "$METADATA_FILE" ]; then
    echo "Copying metadata file to HDFS: $METADATA_HDFS_PATH"
    hdfs dfs -put -f $METADATA_FILE $METADATA_HDFS_PATH/
else
    echo "Warning: Metadata file not found at $METADATA_FILE"
fi

echo "Ingestion completed for date $DATE"
echo "Files available at:"
echo "  - $LOGS_HDFS_PATH"
echo "  - $METADATA_HDFS_PATH"
