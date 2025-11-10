#!/usr/bin/env bash
set -euo pipefail

# Helper script to merge all JSONL outputs from multiple jobs

OUT_DIR="${1:-$PWD/out}"
MERGED_JSONL="${OUT_DIR}/captions_merged.jsonl"
MERGED_TSV="${OUT_DIR}/captions_merged.tsv"

if [ ! -d "$OUT_DIR" ]; then
    echo "Error: Output directory $OUT_DIR does not exist"
    exit 1
fi

# Find all caption JSONL files
JSONL_FILES=("$OUT_DIR"/captions-*.jsonl)

if [ ${#JSONL_FILES[@]} -eq 0 ] || [ ! -e "${JSONL_FILES[0]}" ]; then
    echo "Error: No caption JSONL files found in $OUT_DIR"
    exit 1
fi

echo "Found ${#JSONL_FILES[@]} JSONL files to merge:"
for f in "${JSONL_FILES[@]}"; do
    echo "  - $f"
done

# Run merge script
python3 merge_jsonl.py \
    "${JSONL_FILES[@]}" \
    --output "$MERGED_JSONL" \
    --output-tsv "$MERGED_TSV" \
    --stats

echo ""
echo "Merge complete!"
echo "Output files:"
echo "  JSONL: $MERGED_JSONL"
echo "  TSV:   $MERGED_TSV"
