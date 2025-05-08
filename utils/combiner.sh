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

# Loop through files containing 'monthly' in their name
MONTHLY_DATASET="$OUTPUT_DIR/combined_monthly_dataset.csv"
> "$MONTHLY_DATASET"
for file in "$INPUT_DIR"/*monthly*; do
    # Check if the file exists and is a regular file
    if [ -f "$file" ]; then
        # Skip the first line and append the rest to the output file
        tail -n +2 "$file" >> "$MONTHLY_DATASET"
    fi
done
echo "Combined content saved in $MONTHLY_DATASET"

# Loop through files containing 'yearly' in their name
YEARLY_DATASET="$OUTPUT_DIR/combined_yearly_dataset.csv"
> "$YEARLY_DATASET"
for file in "$INPUT_DIR"/*yearly*; do
    # Check if the file exists and is a regular file
    if [ -f "$file" ]; then
        # Skip the first line and append the rest to the output file
        tail -n +2 "$file" >> "$YEARLY_DATASET"
    fi
done
echo "Combined content saved in $YEARLY_DATASET"

# Loop through files containing 'hourly' in their name
HOURLY_DATASET="$OUTPUT_DIR/combined_hourly_dataset.csv"
> "$HOURLY_DATASET"
for file in "$INPUT_DIR"/*hourly*; do
    # Check if the file exists and is a regular file
    if [ -f "$file" ]; then
        # Skip the first line and append the rest to the output file
        tail -n +2 "$file" >> "$HOURLY_DATASET"
    fi
done
echo "Combined content saved in $HOURLY_DATASET"
