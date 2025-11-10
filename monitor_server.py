#!/usr/bin/env python3
"""
Monitor vLLM server queue status and GPU utilization in real-time.
"""
import argparse
import os
import re
import subprocess
import sys
import time
from datetime import datetime

import requests


def parse_args():
    ap = argparse.ArgumentParser(
        description="Monitor vLLM server queue and GPU utilization"
    )
    ap.add_argument(
        "--url",
        default="http://127.0.0.1:8901",
        help="vLLM server base URL (default: http://127.0.0.1:8901)"
    )
    ap.add_argument(
        "--interval",
        type=int,
        default=2,
        help="Refresh interval in seconds (default: 2)"
    )
    ap.add_argument(
        "--no-gpu",
        action="store_true",
        help="Disable GPU monitoring (metrics only)"
    )
    return ap.parse_args()


def get_vllm_metrics(base_url):
    """Fetch vLLM metrics from /metrics endpoint."""
    try:
        r = requests.get(f"{base_url}/metrics", timeout=5)
        if r.status_code != 200:
            return None

        metrics = {}
        for line in r.text.split('\n'):
            if line.startswith('#') or not line.strip():
                continue

            # Parse prometheus format: metric_name{labels} value
            # We're interested in these metrics:
            # - vllm:num_requests_running
            # - vllm:num_requests_waiting
            # - vllm:gpu_cache_usage_perc
            # - vllm:avg_generation_throughput_toks_per_s
            # - vllm:avg_prompt_throughput_toks_per_s

            if 'vllm:num_requests_running' in line:
                parts = line.split()
                if len(parts) >= 2:
                    metrics['running'] = int(float(parts[-1]))
            elif 'vllm:num_requests_waiting' in line:
                parts = line.split()
                if len(parts) >= 2:
                    metrics['waiting'] = int(float(parts[-1]))
            elif 'vllm:num_requests_swapped' in line:
                parts = line.split()
                if len(parts) >= 2:
                    metrics['swapped'] = int(float(parts[-1]))
            elif 'vllm:gpu_cache_usage_perc' in line:
                parts = line.split()
                if len(parts) >= 2:
                    metrics['gpu_cache_pct'] = float(parts[-1])
            elif 'vllm:avg_generation_throughput_toks_per_s' in line:
                parts = line.split()
                if len(parts) >= 2:
                    metrics['gen_throughput'] = float(parts[-1])
            elif 'vllm:avg_prompt_throughput_toks_per_s' in line:
                parts = line.split()
                if len(parts) >= 2:
                    metrics['prompt_throughput'] = float(parts[-1])

        return metrics
    except Exception as e:
        return {'error': str(e)}


def get_gpu_stats():
    """Get GPU utilization and memory usage from nvidia-smi."""
    try:
        result = subprocess.run(
            ['nvidia-smi', '--query-gpu=utilization.gpu,memory.used,memory.total,temperature.gpu',
             '--format=csv,noheader,nounits'],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode != 0:
            return None

        # Parse output: "85, 120000, 143771, 65"
        lines = result.stdout.strip().split('\n')
        gpus = []
        for line in lines:
            parts = [x.strip() for x in line.split(',')]
            if len(parts) >= 4:
                gpus.append({
                    'util': int(parts[0]),
                    'mem_used': int(parts[1]),
                    'mem_total': int(parts[2]),
                    'temp': int(parts[3])
                })

        return gpus if gpus else None
    except Exception:
        return None


def print_status(metrics, gpu_stats):
    """Pretty-print current status."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{'='*70}")
    print(f"vLLM SERVER MONITOR - {now}")
    print(f"{'='*70}")

    # vLLM metrics
    if metrics is None:
        print("\nvLLM Server: OFFLINE or metrics unavailable")
    elif 'error' in metrics:
        print(f"\nvLLM Server: ERROR - {metrics['error']}")
    else:
        running = metrics.get('running', 0)
        waiting = metrics.get('waiting', 0)
        swapped = metrics.get('swapped', 0)
        total_requests = running + waiting + swapped

        print(f"\nvLLM Queue Status:")
        print(f"  Running requests:  {running:>4}")
        print(f"  Waiting requests:  {waiting:>4}")
        print(f"  Swapped requests:  {swapped:>4}")
        print(f"  Total in queue:    {total_requests:>4}")

        # Visual queue indicator
        if total_requests > 0:
            bar_width = 50
            running_bar = int(bar_width * running / max(total_requests, 1))
            waiting_bar = int(bar_width * waiting / max(total_requests, 1))
            swapped_bar = bar_width - running_bar - waiting_bar

            bar = '█' * running_bar + '▒' * waiting_bar + '░' * swapped_bar
            print(f"  Queue: [{bar}]")
            print(f"         └─ █ Running  ▒ Waiting  ░ Swapped")

        # Cache and throughput
        if 'gpu_cache_pct' in metrics:
            cache_pct = metrics['gpu_cache_pct']
            print(f"\nGPU KV Cache: {cache_pct:.1f}%")

        if 'gen_throughput' in metrics or 'prompt_throughput' in metrics:
            print(f"\nThroughput:")
            if 'prompt_throughput' in metrics:
                print(f"  Prompt:     {metrics['prompt_throughput']:.1f} tokens/s")
            if 'gen_throughput' in metrics:
                print(f"  Generation: {metrics['gen_throughput']:.1f} tokens/s")

    # GPU stats
    if gpu_stats:
        print(f"\nGPU Status:")
        for i, gpu in enumerate(gpu_stats):
            mem_pct = 100 * gpu['mem_used'] / gpu['mem_total']
            print(f"  GPU {i}:")
            print(f"    Utilization: {gpu['util']:>3}%  {'█' * (gpu['util'] // 5)}")
            print(f"    Memory:      {mem_pct:>3.0f}%  ({gpu['mem_used']:,} / {gpu['mem_total']:,} MB)")
            print(f"    Temperature: {gpu['temp']:>3}°C")
    elif gpu_stats is None and not parse_args().no_gpu:
        print(f"\nGPU Status: UNAVAILABLE (nvidia-smi failed)")

    # Warnings
    if metrics and 'error' not in metrics:
        running = metrics.get('running', 0)
        waiting = metrics.get('waiting', 0)
        total = running + waiting

        if total < 100:
            print(f"\n⚠️  WARNING: Queue is low ({total} requests)")
            print(f"   Consider increasing MAX_WORKERS or checking client")
        elif total > 2000:
            print(f"\n⚠️  WARNING: Queue is very high ({total} requests)")
            print(f"   Server may be overloaded or GPU bottlenecked")

    if gpu_stats:
        for gpu in gpu_stats:
            if gpu['util'] < 50:
                print(f"\n⚠️  WARNING: GPU utilization is low ({gpu['util']}%)")
                print(f"   GPU is not fully utilized, may need more requests")
            if gpu['temp'] > 80:
                print(f"\n⚠️  WARNING: GPU temperature is high ({gpu['temp']}°C)")

    print(f"{'='*70}\n")


def main():
    args = parse_args()

    print(f"Monitoring vLLM server at {args.url}")
    print(f"Press Ctrl+C to stop")

    try:
        while True:
            os.system('clear' if os.name == 'posix' else 'cls')

            metrics = get_vllm_metrics(args.url)
            gpu_stats = None if args.no_gpu else get_gpu_stats()

            print_status(metrics, gpu_stats)
            print(f"(Refreshing every {args.interval}s, press Ctrl+C to stop)")

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")


if __name__ == "__main__":
    main()
