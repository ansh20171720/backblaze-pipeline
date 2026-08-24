#!/bin/bash
set -e

RAW_DIR="data/raw"
mkdir -p "$RAW_DIR"

YEARS=(2024 2025)

for YEAR in "${YEARS[@]}"; do
  for Q in 1 2 3 4; do
    FILE="data_Q${Q}_${YEAR}.zip"
    URL="https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/${FILE}"
    DEST="${RAW_DIR}/${FILE}"

    if [ -f "$DEST" ]; then
      echo "Already downloaded: $FILE"
      continue
    fi

    echo "Downloading $FILE..."
    curl -f -o "$DEST" "$URL" && unzip -o "$DEST" -d "$RAW_DIR"
  done
done