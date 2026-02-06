#!/bin/bash

INPUT_DIR="./input"

for file in "$INPUT_DIR"/*; do
  if [ -f "$file" ]; then
    echo "Processing: $file"
    go run main.go -input "$file"
  fi
done
