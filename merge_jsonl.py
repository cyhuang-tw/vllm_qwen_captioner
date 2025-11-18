#!/usr/bin/env python3
"""
Merge multiple JSONL caption output files into a single file.
Handles duplicates by keeping the first successful result or the last attempt.
"""
import argparse
import json
import os
import sys
from collections import defaultdict


def parse_args():
    ap = argparse.ArgumentParser(
        description="Merge multiple JSONL caption files, handling duplicates intelligently"
    )
    ap.add_argument(
        "input_files",
        nargs="+",
        help="Input JSONL files to merge (glob patterns supported)"
    )
    ap.add_argument(
        "--key-field",
        default="utt",
        help="Record key field to de-duplicate on (default: utt). Use 'idx' for arkive outputs."
    )
    ap.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output merged JSONL file"
    )
    ap.add_argument(
        "--prefer-success",
        action="store_true",
        default=True,
        help="Prefer successful results over failed ones (default: True)"
    )
    ap.add_argument(
        "--output-tsv",
        help="Optional: also output merged TSV file (utt<TAB>caption)"
    )
    ap.add_argument(
        "--stats",
        action="store_true",
        help="Print merge statistics"
    )
    return ap.parse_args()


def load_jsonl_records(file_path):
    """Load all records from a JSONL file."""
    records = []
    if not os.path.exists(file_path):
        print(f"Warning: {file_path} does not exist, skipping.", file=sys.stderr)
        return records

    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                records.append(obj)
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse line {line_num} in {file_path}: {e}", file=sys.stderr)
    return records


def merge_records(all_records, key_field="utt", prefer_success=True):
    """
    Merge records, handling duplicates.

    Strategy:
    - If prefer_success=True: keep first successful result for each utt, or last failed attempt
    - Otherwise: keep the last record for each utt
    """
    # Group by key (utt or idx)
    utt_records = defaultdict(list)
    for record in all_records:
        key = record.get(key_field)
        if key is not None:
            utt_records[key].append(record)

    merged = {}
    stats = {
        "total_utts": len(utt_records),
        "successful": 0,
        "failed": 0,
        "duplicates_resolved": 0,
        "missing_key": len(all_records) - sum(len(v) for v in utt_records.values()),
    }

    for utt, records in utt_records.items():
        if len(records) > 1:
            stats["duplicates_resolved"] += len(records) - 1

        if prefer_success:
            # Find first successful record
            chosen = None
            for rec in records:
                if rec.get("status") == "ok":
                    chosen = rec
                    break
            # If no success, take the last failed one
            if chosen is None:
                chosen = records[-1]
        else:
            # Just take the last record
            chosen = records[-1]

        merged[utt] = chosen
        if chosen.get("status") == "ok":
            stats["successful"] += 1
        else:
            stats["failed"] += 1

    return merged, stats


def write_outputs(merged, output_jsonl, output_tsv=None):
    """Write merged results to JSONL and optionally TSV."""
    # Sort by utterance ID for consistent output
    sorted_utts = sorted(merged.keys())

    with open(output_jsonl, "w", encoding="utf-8") as jout:
        for utt in sorted_utts:
            record = merged[utt]
            jout.write(json.dumps(record, ensure_ascii=False) + "\n")

    if output_tsv:
        with open(output_tsv, "w", encoding="utf-8") as tsv:
            for utt in sorted_utts:
                record = merged[utt]
                if record.get("status") == "ok":
                    caption = record.get("caption", "").replace("\n", " ").strip()
                    tsv.write(f"{utt}\t{caption}\n")


def main():
    args = parse_args()

    # Expand glob patterns if any
    import glob
    input_files = []
    for pattern in args.input_files:
        matches = glob.glob(pattern)
        if matches:
            input_files.extend(matches)
        else:
            # Not a glob pattern, use as-is
            input_files.append(pattern)

    print(f"Merging {len(input_files)} files...")
    for f in input_files:
        print(f"  - {f}")

    # Load all records
    all_records = []
    for file_path in input_files:
        records = load_jsonl_records(file_path)
        all_records.extend(records)
        print(f"Loaded {len(records)} records from {file_path}")

    print(f"\nTotal records loaded: {len(all_records)}")

    # Merge records
    merged, stats = merge_records(all_records, key_field=args.key_field, prefer_success=args.prefer_success)

    # Write outputs
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    write_outputs(merged, args.output, args.output_tsv)

    print(f"\n{'='*60}")
    print(f"MERGE COMPLETE")
    print(f"{'='*60}")
    print(f"Total unique utterances: {stats['total_utts']}")
    print(f"Successful: {stats['successful']}")
    print(f"Failed: {stats['failed']}")
    print(f"Duplicates resolved: {stats['duplicates_resolved']}")
    if stats["missing_key"]:
        print(f"Missing key '{args.key_field}' in {stats['missing_key']} records (skipped)")
    print(f"\nOutput written to:")
    print(f"  {args.output}")
    if args.output_tsv:
        print(f"  {args.output_tsv}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
