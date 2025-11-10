#!/usr/bin/env python3
import argparse, base64, json, os, shlex, subprocess, sys, time, mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from threading import Lock
import threading

import requests

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scp", required=True, help="wav.scp path")
    ap.add_argument("--base-url", default="http://127.0.0.1:8901/v1")
    ap.add_argument("--model", required=True)
    ap.add_argument("--out-jsonl", required=True)
    ap.add_argument("--out-tsv", required=True)
    ap.add_argument("--max-workers", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--resume", action="store_true", help="skip utts already in out-jsonl")
    ap.add_argument("--max-retries", type=int, default=3, help="max retry attempts for failed samples")
    ap.add_argument("--checkpoint-interval", type=int, default=100, help="save checkpoint every N samples")
    ap.add_argument("--max-queue-size", type=int, default=200, help="max vLLM queue size before throttling")
    ap.add_argument("--queue-check-interval", type=float, default=2.0, help="seconds between queue checks")
    return ap.parse_args()

def read_wavscp(path):
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"): 
                continue
            # split only once: utt_id and the rest
            parts = ln.split(maxsplit=1)
            if len(parts) != 2:
                continue
            utt, src = parts
            yield utt, src

def load_audio_bytes(src):
    """src is either a file path or a pipe command ending with '|'."""
    if src.endswith("|"):
        cmd = src[:-1].strip()
        # run the pipeline, capture bytes from stdout
        data = subprocess.check_output(cmd, shell=True)
        # assume pipeline outputs WAV
        return data, "audio/wav"
    else:
        with open(src, "rb") as f:
            data = f.read()
        mime, _ = mimetypes.guess_type(src)
        if mime is None:
            # guess WAV if unknown
            mime = "audio/wav"
        return data, mime

def to_data_url(data, mime="audio/wav"):
    b64 = base64.b64encode(data).decode("utf-8")
    return f"data:{mime};base64,{b64}"

def get_queue_size(base_url, timeout=5):
    """Try to get current queue size from vLLM metrics endpoint."""
    try:
        # vLLM exposes metrics at /metrics endpoint
        metrics_url = base_url.replace("/v1", "/metrics")
        r = requests.get(metrics_url, timeout=timeout)
        if r.status_code == 200:
            # Parse prometheus metrics for queue size
            for line in r.text.split('\n'):
                if 'vllm:num_requests_waiting' in line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(float(parts[-1]))
        return None
    except Exception:
        return None

def post_one(session, base_url, model, data_url, timeout):
    payload = {
        "model": model,
        "messages": [{
            "role": "user",
            "content": [{"type": "audio_url", "audio_url": {"url": data_url}}],
        }],
        "temperature": 0.2,
        "max_tokens": 400,
    }
    r = session.post(f"{base_url}/chat/completions", json=payload, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    text = j["choices"][0]["message"]["content"]
    usage = j.get("usage", {})
    return text, usage

def load_done_and_retry_info(out_jsonl):
    """Load completed utterances and retry counts for failed ones."""
    done = set()
    retry_counts = defaultdict(int)
    if not os.path.exists(out_jsonl):
        return done, retry_counts

    with open(out_jsonl, "r", encoding="utf-8") as f:
        for ln in f:
            try:
                obj = json.loads(ln)
                utt = obj.get("utt")
                if obj.get("status") == "ok":
                    done.add(utt)
                    # Remove from retry tracking if it succeeded
                    retry_counts.pop(utt, None)
                elif obj.get("status") == "fail":
                    # Track retry attempts
                    retry_counts[utt] = obj.get("retry_attempt", retry_counts.get(utt, 0) + 1)
            except Exception:
                pass
    return done, retry_counts

class ThroughputTracker:
    """Track processing throughput and estimate GPU hours."""
    def __init__(self):
        self.start_time = time.time()
        self.processed = 0
        self.total_audio_seconds = 0.0
        self.total_processing_time = 0.0
        self.samples_with_duration = 0
        self.samples_without_duration = 0
        self.duration_errors = defaultdict(int)
        self.lock = Lock()

    def update(self, audio_duration_seconds=None, audio_duration_error=None, processing_time=None):
        with self.lock:
            self.processed += 1
            if audio_duration_seconds:
                self.total_audio_seconds += audio_duration_seconds
                self.samples_with_duration += 1
            else:
                self.samples_without_duration += 1
                if audio_duration_error:
                    self.duration_errors[audio_duration_error] += 1
            if processing_time:
                self.total_processing_time += processing_time

    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            if elapsed < 0.1:
                return None
            throughput = self.processed / elapsed  # samples per second (wall clock)
            # RTF = audio duration / wall clock time (for parallel processing)
            # This gives system throughput, not per-request latency
            if self.total_audio_seconds > 0 and elapsed > 0:
                rtf = self.total_audio_seconds / elapsed
            else:
                rtf = None
            return {
                "elapsed_seconds": elapsed,
                "processed": self.processed,
                "throughput_samples_per_sec": throughput,
                "throughput_samples_per_hour": throughput * 3600,
                "real_time_factor": rtf,
                "samples_with_duration": self.samples_with_duration,
                "samples_without_duration": self.samples_without_duration,
                "duration_errors": dict(self.duration_errors),
                "total_processing_time": self.total_processing_time,
                "total_audio_hours": self.total_audio_seconds / 3600
            }

    def estimate_gpu_hours(self, total_samples, librispeech_960h_seconds=960*3600):
        """Estimate GPU hours needed for a dataset."""
        stats = self.get_stats()
        if not stats or stats["processed"] < 10:
            return None

        samples_per_hour = stats["throughput_samples_per_hour"]
        if samples_per_hour < 0.1:
            return None

        # Estimate for total samples
        gpu_hours_for_samples = total_samples / samples_per_hour

        # If we have audio duration info, also estimate by RTF
        if stats["real_time_factor"] and stats["real_time_factor"] > 0:
            # For LibriSpeech 960h
            gpu_hours_for_960h = librispeech_960h_seconds / (stats["real_time_factor"] * 3600)
        else:
            gpu_hours_for_960h = None

        return {
            "gpu_hours_for_total": gpu_hours_for_samples,
            "gpu_hours_for_librispeech_960h": gpu_hours_for_960h
        }

class RateLimiter:
    """Adaptive rate limiter based on vLLM queue size."""
    def __init__(self, base_url, max_queue_size, check_interval):
        self.base_url = base_url
        self.max_queue_size = max_queue_size
        self.check_interval = check_interval
        self.last_check = 0
        self.enabled = True

    def should_throttle(self):
        """Check if we should throttle submission based on queue size."""
        if not self.enabled:
            return False

        now = time.time()
        if now - self.last_check < self.check_interval:
            return False

        self.last_check = now
        queue_size = get_queue_size(self.base_url)
        if queue_size is None:
            # Can't get queue info, disable throttling
            self.enabled = False
            return False

        return queue_size >= self.max_queue_size

def main():
    args = parse_args()
    os.makedirs(os.path.dirname(args.out_jsonl) or ".", exist_ok=True)

    # Load done set and retry counts
    done, retry_counts = load_done_and_retry_info(args.out_jsonl) if args.resume else (set(), defaultdict(int))

    # Filter entries: skip done and max-retried
    all_entries = list(read_wavscp(args.scp))
    entries = []
    for u, s in all_entries:
        if u in done:
            continue
        if retry_counts.get(u, 0) >= args.max_retries:
            print(f"Skipping {u} (max retries {args.max_retries} reached)")
            continue
        entries.append((u, s))

    total = len(entries)
    total_in_scp = len(all_entries)
    print(f"Total in scp: {total_in_scp}")
    print(f"Already done: {len(done)}")
    print(f"Max retries reached: {sum(1 for u, _ in all_entries if retry_counts.get(u, 0) >= args.max_retries)}")
    print(f"To process: {total}")

    if total == 0:
        print("Nothing to process. Done.")
        return

    # Configure session with large connection pool to support 2000 concurrent connections
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=100,      # Number of connection pools
        pool_maxsize=2000,         # Max connections per pool (must match max_workers!)
        max_retries=0,             # No automatic retries (we handle retries ourselves)
        pool_block=False           # Don't block when pool is full, create new connections
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    tracker = ThroughputTracker()

    ok = 0
    fail = 0
    lock = Lock()
    last_checkpoint = 0

    def worker(pair):
        utt, src = pair
        retry_attempt = retry_counts.get(utt, 0) + 1

        start_time = time.time()
        try:
            data, mime = load_audio_bytes(src)
            data_url = to_data_url(data, mime)
            text, usage = post_one(session, args.base_url, args.model, data_url, args.timeout)
            duration = time.time() - start_time

            # Try to get audio duration if available
            audio_duration = None
            audio_duration_error = None
            try:
                import soundfile as sf
                from io import BytesIO

                if src.endswith("|"):
                    # For pipe commands, read from the already-loaded data
                    audio_duration_error = "pipe_command"
                    try:
                        audio_io = BytesIO(data)
                        info = sf.info(audio_io)
                        audio_duration = info.duration
                        audio_duration_error = None
                    except Exception as e:
                        audio_duration_error = f"pipe_decode_error: {str(e)[:50]}"
                else:
                    # For file paths, read directly from file
                    info = sf.info(src)
                    audio_duration = info.duration
            except Exception as e:
                audio_duration_error = f"error: {type(e).__name__}: {str(e)[:50]}"

            tracker.update(audio_duration, audio_duration_error, duration)

            result = {
                "utt": utt,
                "caption": text,
                "usage": usage,
                "status": "ok",
                "processing_time": duration,
                "retry_attempt": retry_attempt,
                "audio_duration": audio_duration
            }
            if audio_duration_error:
                result["audio_duration_error"] = audio_duration_error
            return result
        except Exception as e:
            duration = time.time() - start_time
            return {
                "utt": utt,
                "error": str(e),
                "status": "fail",
                "processing_time": duration,
                "retry_attempt": retry_attempt
            }

    with ThreadPoolExecutor(max_workers=args.max_workers) as ex, \
         open(args.out_jsonl, "a", encoding="utf-8") as jout, \
         open(args.out_tsv, "a", encoding="utf-8") as tsv:

        futures = {ex.submit(worker, p): p for p in entries}

        for fut in as_completed(futures):
            res = fut.result()

            with lock:
                if res["status"] == "ok":
                    ok += 1
                    tsv.write(f"{res['utt']}\t{res['caption'].replace(chr(10),' ').strip()}\n")
                    tsv.flush()
                else:
                    fail += 1

                jout.write(json.dumps(res, ensure_ascii=False) + "\n")
                jout.flush()

                processed = ok + fail

                # Print progress with throughput stats
                stats = tracker.get_stats()
                if stats:
                    throughput_str = f"{stats['throughput_samples_per_sec']:.2f} samples/s"
                    if stats['real_time_factor']:
                        throughput_str += f" (RTF: {stats['real_time_factor']:.2f}x)"
                    print(f"[{ok}/{total} ok, {fail} fail] {res.get('utt')} | {throughput_str}")
                else:
                    print(f"[{ok}/{total} ok, {fail} fail] {res.get('utt')}")

                # Periodic checkpoint report
                if processed - last_checkpoint >= args.checkpoint_interval:
                    last_checkpoint = processed
                    print(f"\n{'='*60}")
                    print(f"CHECKPOINT @ {processed}/{total} processed")
                    if stats:
                        print(f"  Elapsed: {stats['elapsed_seconds']:.1f}s")
                        print(f"  Throughput: {stats['throughput_samples_per_sec']:.3f} samples/s")
                        print(f"  Throughput: {stats['throughput_samples_per_hour']:.1f} samples/hour")

                        # Audio duration stats
                        print(f"  Total audio processed: {stats['total_audio_hours']:.2f} hours")
                        print(f"  Samples with duration: {stats['samples_with_duration']}/{stats['processed']} ({100*stats['samples_with_duration']/max(stats['processed'],1):.1f}%)")

                        # Show duration error breakdown if there are missing durations
                        if stats['samples_without_duration'] > 0:
                            print(f"  ⚠ Samples without duration: {stats['samples_without_duration']} ({100*stats['samples_without_duration']/max(stats['processed'],1):.1f}%)")
                            if stats['duration_errors']:
                                print(f"    Reasons:")
                                for error_type, count in sorted(stats['duration_errors'].items(), key=lambda x: x[1], reverse=True):
                                    print(f"      - {error_type}: {count}")

                        if stats['real_time_factor']:
                            print(f"  Real-time factor: {stats['real_time_factor']:.2f}x")
                            print(f"  (System processed {stats['total_audio_hours']:.2f}h audio in {stats['elapsed_seconds']/3600:.2f}h wall clock time)")
                        else:
                            print(f"  Real-time factor: N/A (no audio duration data)")

                        # Estimate GPU hours
                        estimates = tracker.estimate_gpu_hours(total_in_scp)
                        if estimates:
                            print(f"\n  GPU Hour Estimates (1 GPU, multiply by TP size if applicable):")
                            print(f"    For this dataset ({total_in_scp} samples): {estimates['gpu_hours_for_total']:.2f} GPU hours")
                            if estimates['gpu_hours_for_librispeech_960h']:
                                print(f"    For LibriSpeech 960h: {estimates['gpu_hours_for_librispeech_960h']:.2f} GPU hours")
                    print(f"{'='*60}\n")

    # Final report
    print(f"\n{'='*60}")
    print(f"FINAL REPORT")
    print(f"{'='*60}")
    print(f"Total processed: {ok + fail}")
    print(f"Successful: {ok}")
    print(f"Failed: {fail}")

    stats = tracker.get_stats()
    if stats:
        print(f"\nThroughput Statistics:")
        print(f"  Wall clock time: {stats['elapsed_seconds']:.1f}s ({stats['elapsed_seconds']/3600:.2f}h)")
        print(f"  Throughput: {stats['throughput_samples_per_sec']:.3f} samples/s (wall clock)")
        print(f"  Throughput: {stats['throughput_samples_per_hour']:.1f} samples/hour (wall clock)")

        if stats['real_time_factor']:
            print(f"\nAudio Processing:")
            print(f"  Total audio processed: {stats['total_audio_hours']:.2f} hours")
            print(f"  Wall clock time: {stats['elapsed_seconds']/3600:.2f} hours")
            print(f"  Real-time factor: {stats['real_time_factor']:.2f}x")
            print(f"  (System processed audio {stats['real_time_factor']:.2f}x faster than real-time)")

        estimates = tracker.estimate_gpu_hours(total_in_scp)
        if estimates:
            print(f"\nGPU Hour Estimates (for 1 GPU, multiply by TP size if using tensor parallelism):")
            print(f"  For this dataset ({total_in_scp} samples): {estimates['gpu_hours_for_total']:.2f} GPU hours")
            if estimates['gpu_hours_for_librispeech_960h']:
                print(f"  For LibriSpeech 960h (960 hours audio): {estimates['gpu_hours_for_librispeech_960h']:.2f} GPU hours")

    print(f"\nOutput files:")
    print(f"  {args.out_jsonl}")
    print(f"  {args.out_tsv}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()

