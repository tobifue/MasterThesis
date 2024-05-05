
#!/bin/bash

# Iterate over all CSV files in the current directory
for file in *.csv; do
    # Check if the file exists and is a regular file
    if [ -f "$file" ]; then
        # Execute the move.sh script for each CSV file
        ./move.sh "$file"
    fi
done







