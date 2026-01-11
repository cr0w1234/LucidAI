#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://dev-chsa-ai.pantheonsite.io/wp-json/wp/v2"
OUTPUT_DIR="$CSHA_BASE_DATA_DIR/raw/website-data/endpoint-metadata"
mkdir -p "$OUTPUT_DIR"

# Top‑level endpoints
ENDPOINTS=(
  posts
  pages
  media
  menu-items
  blocks
  templates
  template-parts
  navigation
  font-families
  person
  resource
)

echo "Fetching metadata for top‑level endpoints..."
for base in "${ENDPOINTS[@]}"; do
  curl -s -X OPTIONS "$BASE_URL/$base" \
    | jq '.' \
    > "$OUTPUT_DIR/${base}_endpoint_metadata.json"
  echo "  • $base → ${base}_endpoint_metadata.json"
done

echo

echo "Metadata fetch complete. Files in $OUTPUT_DIR."