#!/usr/bin/env python3
"""
Monitor progress and throughput from a running or completed caption job.
"""
import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime


def parse_args():
    ap = argparse.ArgumentParser(
        description="Monitor progress and throughput of caption jobs"
    )
    ap.add_argument(
        "jsonl_file",
        help="Path to the output JSONL file to monitor"
    )
    ap.add_argument(
        "--scp",
        help="Path to wav.scp to get total count automatically"
    )
    ap.add_argument(
        "--total",
        type=int,
        help="Total number of utterances (for progress percentage)"
    )
    ap.add_argument(
        "--watch",
        action="store_true",
        help="Continuously monitor (refresh every 5 seconds)"
    )
    ap.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Refresh interval in seconds for watch mode (default: 5)"
    )
    return ap.parse_args()


def count_scp_lines(scp_file):
    """Count total utterances in wav.scp file."""
    if not os.path.exists(scp_file):
        return None
    count = 0
    with open(scp_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                count += 1
    return count


def analyze_jsonl(jsonl_file):
    """Analyze a JSONL file and return statistics."""
    if not os.path.exists(jsonl_file):
        return None

    stats = {
        "total_lines": 0,
        "unique_utts": set(),
        "successful_utts": set(),
        "failed_utts": set(),
        "total_processing_time": 0.0,
        "total_audio_duration": 0.0,
        "retry_counts": defaultdict(int),
        "error_types": defaultdict(int),
        "last_utt": None,
        "last_status": None,
        "recent_throughput": [],  # (timestamp, count) for last 100 items
    }

    file_size = os.path.getsize(jsonl_file)
    file_ctime = os.path.getctime(jsonl_file)  # File creation time (job start proxy)
    file_mtime = os.path.getmtime(jsonl_file)  # Last modification (job end proxy)

    with open(jsonl_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                stats["total_lines"] += 1

                utt = obj.get("utt")
                status = obj.get("status")
                stats["unique_utts"].add(utt)

                if status == "ok":
                    stats["successful_utts"].add(utt)
                    stats["failed_utts"].discard(utt)  # Remove if previously failed
                else:
                    stats["failed_utts"].add(utt)
                    error = obj.get("error", "Unknown error")
                    # Extract error type (first 100 chars)
                    error_type = error[:100]
                    stats["error_types"][error_type] += 1

                # Accumulate timing info
                proc_time = obj.get("processing_time", 0)
                if proc_time > 0:
                    stats["total_processing_time"] += proc_time

                audio_dur = obj.get("audio_duration")
                if audio_dur:
                    stats["total_audio_duration"] += audio_dur

                # Track retries
                retry = obj.get("retry_attempt", 1)
                stats["retry_counts"][retry] += 1

                # Track last processed
                stats["last_utt"] = utt
                stats["last_status"] = status

            except json.JSONDecodeError:
                pass

    stats["file_size"] = file_size
    stats["file_ctime"] = file_ctime
    stats["file_mtime"] = file_mtime

    return stats


def print_stats(stats, total_expected=None, prev_stats=None):
    """Pretty-print statistics."""
    if stats is None:
        print("No data available yet.")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_mtime = datetime.fromtimestamp(stats['file_mtime']).strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{'='*70}")
    print(f"PROGRESS REPORT - {now}")
    print(f"{'='*70}")

    # File info
    print(f"File: {stats.get('filename', 'N/A')}")
    print(f"Size: {stats['file_size']/1024/1024:.2f} MB  |  Last modified: {file_mtime}")

    # Unique utterances (actual progress)
    unique_count = len(stats['unique_utts'])
    successful_count = len(stats['successful_utts'])
    failed_count = len(stats['failed_utts'])

    print(f"\nUnique utterances: {unique_count}")
    print(f"  Successful: {successful_count} ({100*successful_count/max(unique_count,1):.1f}%)")
    print(f"  Failed: {failed_count} ({100*failed_count/max(unique_count,1):.1f}%)")
    print(f"Total lines in file: {stats['total_lines']} (includes retries)")

    # Progress bar
    if total_expected:
        pct = 100 * successful_count / total_expected
        remaining = total_expected - successful_count
        bar_width = 40
        filled = int(bar_width * successful_count / total_expected)
        bar = '█' * filled + '░' * (bar_width - filled)
        print(f"\nProgress: [{bar}] {pct:.1f}%")
        print(f"Completed: {successful_count}/{total_expected}")
        print(f"Remaining: {remaining}")

    # Retry statistics
    if stats["retry_counts"]:
        print(f"\nRetry distribution:")
        for retry_num in sorted(stats["retry_counts"].keys()):
            count = stats["retry_counts"][retry_num]
            print(f"  Attempt {retry_num}: {count} ({100*count/max(stats['total_lines'],1):.1f}%)")

    # Throughput
    if stats["total_lines"] > 0 and stats["total_processing_time"] > 0:
        avg_time = stats["total_processing_time"] / stats["total_lines"]
        throughput = 1.0 / avg_time if avg_time > 0 else 0

        print(f"\nThroughput:")
        print(f"  Average: {avg_time:.2f}s/sample  |  {throughput:.2f} samples/s  |  {throughput * 3600:.0f} samples/h")

        # Recent speed (if we have previous stats)
        if prev_stats:
            delta_lines = stats['total_lines'] - prev_stats['total_lines']
            delta_time = stats['file_mtime'] - prev_stats['file_mtime']
            if delta_time > 0 and delta_lines > 0:
                recent_throughput = delta_lines / delta_time
                print(f"  Recent: {recent_throughput:.2f} samples/s  |  {recent_throughput * 3600:.0f} samples/h")

        # ETA
        if total_expected and throughput > 0:
            remaining = total_expected - successful_count
            eta_seconds = remaining / throughput
            eta_hours = eta_seconds / 3600
            eta_days = eta_hours / 24
            if eta_hours < 1:
                print(f"  ETA: {eta_seconds/60:.0f} minutes")
            elif eta_hours < 24:
                print(f"  ETA: {eta_hours:.1f} hours")
            else:
                print(f"  ETA: {eta_days:.1f} days ({eta_hours:.1f} hours)")

    # Real-time factor (using wall clock time from file timestamps)
    if stats["total_audio_duration"] > 0:
        # Use file creation time to file modification time as proxy for wall clock duration
        wall_clock_duration = stats["file_mtime"] - stats["file_ctime"]
        if wall_clock_duration > 0:
            rtf = stats["total_audio_duration"] / wall_clock_duration
            print(f"\nReal-time factor: {rtf:.2f}x")
            print(f"Audio processed: {stats['total_audio_duration']/3600:.2f} hours")
            print(f"Wall clock time: {wall_clock_duration/3600:.2f} hours (based on file timestamps)")

            # GPU hour estimate for LibriSpeech 960h
            if rtf > 0:
                librispeech_hours = 960
                gpu_hours_960h = librispeech_hours / rtf
                print(f"Est. for LibriSpeech 960h: {gpu_hours_960h:.1f} GPU hours")

    # Last processed
    if stats["last_utt"]:
        status_emoji = "✓" if stats["last_status"] == "ok" else "✗"
        print(f"\nLast processed: {stats['last_utt']} {status_emoji}")

    # Error summary
    if stats["error_types"]:
        print(f"\nError summary ({len(stats['error_types'])} unique error types):")
        sorted_errors = sorted(stats["error_types"].items(), key=lambda x: x[1], reverse=True)
        for i, (error, count) in enumerate(sorted_errors[:5], 1):
            error_short = error[:60] + "..." if len(error) > 60 else error
            print(f"  {i}. [{count}x] {error_short}")
        if len(sorted_errors) > 5:
            print(f"  ... and {len(sorted_errors) - 5} more error types")

    print(f"{'='*70}\n")


def main():
    args = parse_args()

    # Get total count from scp if provided
    total_expected = args.total
    if args.scp and total_expected is None:
        total_expected = count_scp_lines(args.scp)
        if total_expected:
            print(f"Auto-detected {total_expected} utterances from {args.scp}")

    if args.watch:
        prev_stats = None
        try:
            while True:
                os.system('clear' if os.name == 'posix' else 'cls')
                stats = analyze_jsonl(args.jsonl_file)
                if stats:
                    stats['filename'] = os.path.basename(args.jsonl_file)
                print_stats(stats, total_expected, prev_stats)
                print(f"(Refreshing every {args.interval}s, press Ctrl+C to stop)")
                prev_stats = stats
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
    else:
        stats = analyze_jsonl(args.jsonl_file)
        if stats:
            stats['filename'] = os.path.basename(args.jsonl_file)
        print_stats(stats, total_expected)


if __name__ == "__main__":
    main()
