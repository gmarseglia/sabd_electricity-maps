#!/bin/bash

URL_FILE="datasets_url.txt"
DATASET_DIR="../dataset/raw"

# Ensure the script gets the file as a parameter
if [ -z "$URL_FILE" ]; then
  echo "$URL_FILE does not exists."
  exit 1
fi

# Ensure the $DATASET_DIR directory exists
if [ ! -d "$DATASET_DIR" ]; then
  echo "Directory $DATASET_DIR does not exist. Creating it..."
  mkdir -p "$DATASET_DIR"
fi

# Read the file line by line
while IFS= read -r line; do
  # Skip lines that start with # (comments)
  if [[ ! "$line" =~ ^# ]]; then
    # Check if the line is not empty
    if [[ -n "$line" ]]; then
      # Check if the file exists already
      echo "Downloading: $line"
      wget -nc -P "$DATASET_DIR" "$line"
    fi
  fi
done < "$URL_FILE"
