#!/usr/bin/env python3
"""
Estimate GPU hours needed for processing various datasets based on completed runs.
"""
import argparse
import json
import sys


def parse_args():
    ap = argparse.ArgumentParser(
        description="Estimate GPU hours needed for datasets based on completed runs"
    )
    ap.add_argument(
        "jsonl_file",
        help="Path to a completed JSONL output file"
    )
    ap.add_argument(
        "--dataset-hours",
        nargs="+",
        type=float,
        default=[960, 100, 500, 1000, 5000],
        help="Audio hours in target datasets (e.g., 960 for LibriSpeech)"
    )
    ap.add_argument(
        "--dataset-names",
        nargs="+",
        default=["LibriSpeech-960h", "100h", "500h", "1000h", "5000h"],
        help="Names for the datasets"
    )
    return ap.parse_args()


def analyze_jsonl(jsonl_file):
    """Calculate throughput metrics from JSONL."""
    total_processing_time = 0.0
    total_audio_duration = 0.0
    successful = 0

    with open(jsonl_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if obj.get("status") != "ok":
                    continue

                successful += 1
                proc_time = obj.get("processing_time", 0)
                total_processing_time += proc_time

                audio_dur = obj.get("audio_duration")
                if audio_dur:
                    total_audio_duration += audio_dur

            except json.JSONDecodeError:
                pass

    if successful == 0 or total_processing_time == 0:
        return None

    # Calculate RTF (real-time factor)
    if total_audio_duration > 0:
        rtf = total_audio_duration / total_processing_time
    else:
        rtf = None

    return {
        "successful": successful,
        "total_processing_time": total_processing_time,
        "total_audio_duration": total_audio_duration,
        "rtf": rtf,
        "avg_processing_time": total_processing_time / successful,
    }


def estimate_for_dataset(metrics, dataset_hours):
    """Estimate GPU hours needed for a dataset."""
    if metrics["rtf"] is None or metrics["rtf"] <= 0:
        return None

    # Dataset audio in seconds
    dataset_seconds = dataset_hours * 3600

    # Processing time needed
    processing_seconds = dataset_seconds / metrics["rtf"]
    gpu_hours = processing_seconds / 3600

    return gpu_hours


def main():
    args = parse_args()

    print(f"Analyzing {args.jsonl_file}...")
    metrics = analyze_jsonl(args.jsonl_file)

    if metrics is None:
        print("Error: Could not extract metrics from JSONL file.", file=sys.stderr)
        print("Make sure the file contains successful results with timing info.", file=sys.stderr)
        sys.exit(1)

    print(f"\n{'='*70}")
    print(f"THROUGHPUT METRICS FROM COMPLETED RUN")
    print(f"{'='*70}")
    print(f"Successful samples: {metrics['successful']}")
    print(f"Total processing time: {metrics['total_processing_time']:.1f}s ({metrics['total_processing_time']/3600:.2f}h)")
    print(f"Average processing time: {metrics['avg_processing_time']:.3f}s/sample")

    if metrics['total_audio_duration'] > 0:
        print(f"Total audio processed: {metrics['total_audio_duration']:.1f}s ({metrics['total_audio_duration']/3600:.2f}h)")
        print(f"Real-time factor (RTF): {metrics['rtf']:.2f}x")
    else:
        print("Warning: No audio duration info available, estimates will be limited")

    if metrics['rtf'] and metrics['rtf'] > 0:
        print(f"\n{'='*70}")
        print(f"GPU HOUR ESTIMATES FOR VARIOUS DATASETS")
        print(f"{'='*70}")
        print(f"{'Dataset':<25} {'Audio Hours':>12} {'GPU Hours':>12}")
        print(f"{'-'*70}")

        for name, hours in zip(args.dataset_names, args.dataset_hours):
            gpu_hours = estimate_for_dataset(metrics, hours)
            if gpu_hours:
                print(f"{name:<25} {hours:>12.1f} {gpu_hours:>12.2f}")

        # Also show in terms of samples
        samples_per_hour = 3600 / metrics['avg_processing_time']
        print(f"\n{'='*70}")
        print(f"THROUGHPUT RATE")
        print(f"{'='*70}")
        print(f"Samples per second: {1/metrics['avg_processing_time']:.2f}")
        print(f"Samples per hour: {samples_per_hour:.1f}")
        print(f"Samples per GPU-day: {samples_per_hour * 24:.0f}")
        print(f"{'='*70}\n")
    else:
        print("\nCannot estimate GPU hours without audio duration information.")


if __name__ == "__main__":
    main()
