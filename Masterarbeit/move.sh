#!/bin/bash

# Define the filename variable
file="$1"

# Check if the filename argument is provided
if [ -z "$file" ]; then
    echo "Usage: $0 <filename>"
    exit 1
fi

# Upload the CSV file to HDFS, replacing if it already exists
hdfs dfs -put -f "$file" /test/test/

# List the files in the destination directory
hdfs dfs -ls /test/test

# Change the permissions of the uploaded file
chmod 777 "$file"

