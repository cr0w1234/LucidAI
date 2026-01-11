#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://www.schoolhealthcenters.org//wp-json/wp/v2"
OUTPUT_DIR="$CSHA_BASE_DATA_DIR/raw/website-data/endpoint-content"
mkdir -p "$OUTPUT_DIR"

# Top‑level endpoints
ENDPOINTS=(
  posts
  pages
  person
  resource
)

echo "Fetching content for top‑level endpoints..."
for base in "${ENDPOINTS[@]}"; do
  # Determine how many pages of results exist
  total_pages=$(curl -s -I "$BASE_URL/$base?per_page=1&page=1" \
                | awk '/^X-WP-TotalPages:/ {print $2}' \
                | tr -d $'\r')
  total_pages=${total_pages:-1}

  echo "  • $base has $total_pages page(s)"
  # Download and merge all pages into one JSON array
  all=$(for p in $(seq 1 "$total_pages"); do
    curl -s "$BASE_URL/$base?per_page=100&page=$p"
  done | jq -s 'add')

  echo "$all" > "$OUTPUT_DIR/${base}_endpoint_content.json"
  echo "    → ${base}_endpoint_content.json"
done

echo

echo "Content fetch complete. Files in $OUTPUT_DIR."