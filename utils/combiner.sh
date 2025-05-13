#!/bin/bash

# Create or empty the output file
INPUT_DIR="../dataset/raw"
OUTPUT_DIR="../dataset/combined"

# Ensure the $INPUT_DIR directory exists
if [ ! -d "$INPUT_DIR" ]; then
  echo "Directory $INPUT_DIR does not exist. Exiting."
  exit 1
fi

# Ensure the $OUTPUT_DIR directory exists
if [ ! -d "$OUTPUT_DIR" ]; then
  echo "Directory $OUTPUT_DIR does not exist. Creating it..."
  mkdir -p "$OUTPUT_DIR"
fi

# Loop through files containing 'IT' and 'hourly' in their name
HOURLY_ITALY_DATASET="$OUTPUT_DIR/combined_dataset-italy_hourly.csv"
> "$HOURLY_ITALY_DATASET"
for file in "$INPUT_DIR"/*IT*hourly*; do
    # Check if the file exists and is a regular file
    if [ -f "$file" ]; then
        # Skip the first line and append the rest to the output file
        tail -n +2 "$file" >> "$HOURLY_ITALY_DATASET"
    fi
done

# Loop through files containing 'IT' and 'hourly' in their name
HOURLY_SWEDEN_DATASET="$OUTPUT_DIR/combined_dataset-sweden_hourly.csv"
> "$HOURLY_SWEDEN_DATASET"
for file in "$INPUT_DIR"/*SE*hourly*; do
    # Check if the file exists and is a regular file
    if [ -f "$file" ]; then
        # Skip the first line and append the rest to the output file
        tail -n +2 "$file" >> "$HOURLY_SWEDEN_DATASET"
    fi
done


echo "Combined dataset saved."
