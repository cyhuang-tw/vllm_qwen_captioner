#!/usr/bin/env python3
import argparse, base64, json, os, sys, time, io, random
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from threading import Lock
from pathlib import Path
import mmap, glob, math
from typing import List, Tuple, Optional

import requests
import pyarrow.parquet as pq
import numpy as np

# Try torchaudio first; fall back to soundfile
USE_TORCHAUDIO = True
try:
    import torch
    import torchaudio
except Exception:
    USE_TORCHAUDIO = False
    import soundfile as sf

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arkdir", required=True, help="Path to arkive directory containing arkive_*.bin and metadata.parquet")
    ap.add_argument("--base-url", default="http://127.0.0.1:8901/v1")
    ap.add_argument("--model", required=True)
    ap.add_argument("--out-jsonl", required=True)
    ap.add_argument("--out-tsv", required=True)
    ap.add_argument("--max-workers", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--resume", action="store_true", help="skip indices already in out-jsonl")
    ap.add_argument("--max-retries", type=int, default=3, help="max retry attempts for failed samples")
    ap.add_argument("--checkpoint-interval", type=int, default=100, help="save checkpoint every N samples")
    ap.add_argument("--start-idx", type=int, default=0, help="start processing from this index")
    ap.add_argument("--end-idx", type=int, default=None, help="end processing at this index (exclusive)")
    ap.add_argument("--max-tokens", type=int, default=400, help="max new tokens to generate for each caption")
    ap.add_argument("--temperature", type=float, default=0.2, help="sampling temperature")
    ap.add_argument("--rtf-sample-rate", type=float, default=0.0, help="probability [0,1] of decoding for RTF; 0 disables decoding")
    return ap.parse_args()

# ---------------------------
# Arkive Reader Classes
# ---------------------------
def open_mmaps(arkdir: Path) -> Tuple[List[mmap.mmap], List[int]]:
    """Open memory-mapped files and return mmaps and file descriptors.

    Returns:
        Tuple of (mmaps, file_descriptors)
    """
    mmaps = []
    fds = []
    shard_paths = sorted(glob.glob(str(arkdir / "arkive_*.bin")))
    if not shard_paths:
        raise FileNotFoundError(f"No shard files found matching arkive_*.bin in {arkdir}")
    for p in shard_paths:
        fd = os.open(p, os.O_RDONLY)
        fds.append(fd)
        mmaps.append(mmap.mmap(fd, 0, access=mmap.ACCESS_READ))
    return mmaps, fds

def count_shard_files(arkdir: Path) -> int:
    """Count number of shard files."""
    shard_paths = glob.glob(str(arkdir / "arkive_*.bin"))
    return len(shard_paths)

def decode_bytes_to_waveform(chunk: bytes):
    """Returns (waveform, sample_rate).
       waveform is torch.Tensor (C, T) if torchaudio is available, else numpy.ndarray (C, T).
    """
    bio = io.BytesIO(chunk)
    if USE_TORCHAUDIO:
        wav_t, sr = torchaudio.load(bio)  # Tensor[C, T], float32
        return wav_t, int(sr)
    else:
        data, sr = sf.read(bio, dtype="float32", always_2d=True)  # (T, C)
        wav_np = np.transpose(data, (1, 0)).copy()
        return wav_np, int(sr)

class ArkiveReader:
    """Random access reader. Each instance owns its own mmaps."""
    def __init__(self, arkdir: Path, index_array: np.ndarray):
        self.arkdir = arkdir
        self.index = index_array
        self.mmaps, self.fds = open_mmaps(arkdir)

    def __len__(self):
        return len(self.index)

    def load(self, i: int):
        """Random access by global index i. Returns (waveform, sr, meta_dict)."""
        rec = self.index[i]
        shard = int(rec["bin_index"])
        off   = int(rec["start"])
        size  = int(rec["size"])
        chunk = self.mmaps[shard][off:off+size]
        waveform, sr = decode_bytes_to_waveform(chunk)
        meta = dict(
            idx=i, bin_index=shard, start=off, size=size,
            sample_rate=int(rec["sr"]), channels=int(rec["ch"]),
            decoded_sr=sr
        )
        return waveform, sr, meta

    def close(self):
        """Close mmaps and file descriptors."""
        # Use getattr with default to handle partial initialization
        for m in getattr(self, 'mmaps', []):
            try:
                m.close()
            except Exception:
                pass
        for fd in getattr(self, 'fds', []):
            try:
                os.close(fd)
            except Exception:
                pass

    def __del__(self):
        """Cleanup on deletion."""
        try:
            self.close()
        except Exception:
            pass  # Silently ignore errors during cleanup

def build_index(arkdir: Path):
    """Build compact index from parquet metadata."""
    parquet_path = arkdir / "metadata.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet metadata not found: {parquet_path}")

    need_cols = ["bin_index", "start_byte_offset", "file_size_bytes",
                 "sample_rate", "channels"]
    pf = pq.ParquetFile(parquet_path)
    table = pq.read_table(parquet_path, columns=[c for c in need_cols if c in pf.schema.names])
    rows = table.to_pylist()

    # Deterministic order
    rows.sort(key=lambda r: (int(r["bin_index"]), int(r["start_byte_offset"])))
    N = len(rows)

    # Create compact numpy struct array for fast indexing
    index_dtype = np.dtype([
        ("bin_index", np.int64),
        ("start",    np.int64),
        ("size",     np.int64),
        ("sr",       np.int64),
        ("ch",       np.int64),
    ])
    index = np.empty(N, dtype=index_dtype)
    for i, r in enumerate(rows):
        index[i]["bin_index"] = int(r["bin_index"])
        index[i]["start"]     = int(r["start_byte_offset"])
        index[i]["size"]      = int(r["file_size_bytes"])
        index[i]["sr"]        = int(r["sample_rate"]) if r.get("sample_rate") is not None else -1
        index[i]["ch"]        = int(r["channels"])    if r.get("channels") is not None else -1

    return index, N

# ---------------------------
# Audio encoding to WAV bytes
# ---------------------------
def waveform_to_wav_bytes(waveform, sample_rate: int) -> bytes:
    """Convert waveform (torch.Tensor or numpy.ndarray) to WAV bytes.
    Input: waveform shape (C, T) where C is channels, T is time samples.
    """
    bio = io.BytesIO()

    if USE_TORCHAUDIO:
        # Convert to torch tensor if needed
        if isinstance(waveform, np.ndarray):
            waveform = torch.from_numpy(waveform)
        # torchaudio.save expects (C, T)
        torchaudio.save(bio, waveform, sample_rate, format='wav')
    else:
        # soundfile expects (T, C)
        # When using soundfile, waveform will be numpy array (not torch.Tensor)
        # since decode_bytes_to_waveform returns numpy when USE_TORCHAUDIO is False
        if not isinstance(waveform, np.ndarray):
            raise TypeError(f"Expected numpy array, got {type(waveform)}")
        # Transpose from (C, T) to (T, C)
        waveform_t = np.transpose(waveform, (1, 0))
        sf.write(bio, waveform_t, sample_rate, format='wav')

    return bio.getvalue()

def detect_audio_mime(data: bytes) -> str:
    """Detect audio MIME type from magic bytes."""
    if data[:4] == b'fLaC':
        return "audio/flac"
    elif data[:3] == b'ID3' or (len(data) > 1 and data[0:2] == b'\xff\xfb'):
        return "audio/mpeg"  # MP3
    elif data[:4] == b'OggS':
        return "audio/ogg"
    elif data[:4] == b'RIFF' and data[8:12] == b'WAVE':
        return "audio/wav"
    elif data[:4] == b'ftyp':
        return "audio/mp4"
    else:
        # Default to generic audio
        return "audio/wav"  # Fallback

def to_data_url(data: bytes, mime="audio/wav") -> str:
    b64 = base64.b64encode(data).decode("utf-8")
    return f"data:{mime};base64,{b64}"

# ---------------------------
# vLLM API
# ---------------------------
def post_one(session, base_url, model, data_url, timeout, temperature, max_tokens):
    payload = {
        "model": model,
        "messages": [{
            "role": "user",
            "content": [{"type": "audio_url", "audio_url": {"url": data_url}}],
        }],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    r = session.post(f"{base_url}/chat/completions", json=payload, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    text = j["choices"][0]["message"]["content"]
    usage = j.get("usage", {})
    return text, usage

# ---------------------------
# Resume/checkpoint handling
# ---------------------------
def load_done_and_retry_info(out_jsonl):
    """Load completed indices and retry counts for failed ones."""
    done = set()
    retry_counts = defaultdict(int)
    if not os.path.exists(out_jsonl):
        return done, retry_counts

    with open(out_jsonl, "r", encoding="utf-8") as f:
        for ln in f:
            try:
                obj = json.loads(ln)
                idx = obj.get("idx")
                if obj.get("status") == "ok":
                    done.add(idx)
                    retry_counts.pop(idx, None)
                elif obj.get("status") == "fail":
                    retry_counts[idx] = obj.get("retry_attempt", retry_counts.get(idx, 0) + 1)
            except Exception:
                pass
    return done, retry_counts

# ---------------------------
# Throughput tracking
# ---------------------------
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

    def update(self, audio_duration_seconds=None, processing_time=None, sample_weight=1.0, audio_duration_error=None):
        with self.lock:
            self.processed += 1
            if audio_duration_seconds:
                # If duration was sampled (e.g., only on a subset), up-weight to estimate total audio processed
                self.total_audio_seconds += audio_duration_seconds * sample_weight
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
            throughput = self.processed / elapsed
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

        gpu_hours_for_samples = total_samples / samples_per_hour

        if stats["real_time_factor"] and stats["real_time_factor"] > 0:
            gpu_hours_for_960h = librispeech_960h_seconds / (stats["real_time_factor"] * 3600)
        else:
            gpu_hours_for_960h = None

        return {
            "gpu_hours_for_total": gpu_hours_for_samples,
            "gpu_hours_for_librispeech_960h": gpu_hours_for_960h
        }

# ---------------------------
# Main processing
# ---------------------------
def main():
    args = parse_args()
    os.makedirs(os.path.dirname(args.out_jsonl) or ".", exist_ok=True)

    # Build index from arkive
    arkdir = Path(args.arkdir)
    print(f"Building index from {arkdir}...")

    num_shards = count_shard_files(arkdir)
    print(f"Found {num_shards} shard files")

    index, total_count = build_index(arkdir)
    print(f"Total audios in arkive: {total_count}")

    # Determine processing range
    start_idx = args.start_idx
    end_idx = args.end_idx if args.end_idx is not None else total_count
    end_idx = min(end_idx, total_count)

    if start_idx >= end_idx:
        print(f"Invalid range: start_idx={start_idx} >= end_idx={end_idx}")
        return

    # Load done set and retry counts
    done, retry_counts = load_done_and_retry_info(args.out_jsonl) if args.resume else (set(), defaultdict(int))

    # Filter indices: skip done and max-retried
    all_indices = list(range(start_idx, end_idx))
    indices = []
    for idx in all_indices:
        if idx in done:
            continue
        if retry_counts.get(idx, 0) >= args.max_retries:
            print(f"Skipping index {idx} (max retries {args.max_retries} reached)")
            continue
        indices.append(idx)

    total = len(indices)
    total_in_range = len(all_indices)
    print(f"Processing range: [{start_idx}, {end_idx})")
    print(f"Total in range: {total_in_range}")
    print(f"Already done: {len(done)}")
    print(f"Max retries reached: {sum(1 for i in all_indices if retry_counts.get(i, 0) >= args.max_retries)}")
    print(f"To process: {total}")

    if total == 0:
        print("Nothing to process. Done.")
        return

    print(f"\n[INFO] Initializing processing with {args.max_workers} workers...")

    # Configure session with large connection pool
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=100,
        pool_maxsize=max(args.max_workers, 2000),
        max_retries=0,
        pool_block=False
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    tracker = ThroughputTracker()

    ok = 0
    fail = 0
    lock = Lock()
    last_checkpoint = 0
    write_counter = 0  # Track writes for periodic flushing

    # Open shard mmaps once and share across all workers (minimal FDs + memory)
    mmaps, fds = open_mmaps(arkdir)
    print(f"[INFO] Opened {len(mmaps)} shard mmaps (shared across all workers)")

    def close_mmaps():
        for m in mmaps:
            try:
                m.close()
            except Exception:
                pass
        for fd in fds:
            try:
                os.close(fd)
            except Exception:
                pass

    def worker(idx):
        retry_attempt = retry_counts.get(idx, 0) + 1
        start_time = time.time()

        try:
            # Get raw audio bytes directly from arkive (zero-copy via shared mmap)
            rec = index[idx]
            shard = int(rec["bin_index"])
            off   = int(rec["start"])
            size  = int(rec["size"])

            # Zero-copy view into mmap for base64; avoids extra allocation
            audio_view = memoryview(mmaps[shard])[off:off+size]

            # Detect mime type from magic bytes
            mime = detect_audio_mime(audio_view)

            # Get sample rate from metadata
            sr = int(rec["sr"])

            # Encode original compressed bytes as data URL
            data_url = to_data_url(audio_view, mime)

            # Post to vLLM
            text, usage = post_one(
                session,
                args.base_url,
                args.model,
                data_url,
                args.timeout,
                args.temperature,
                args.max_tokens,
            )
            duration = time.time() - start_time

            # Optionally sample audio duration for RTF (avoids decoding every sample)
            audio_duration = None
            sample_weight = 1.0
            if args.rtf_sample_rate > 0 and random.random() < args.rtf_sample_rate:
                try:
                    waveform, decoded_sr = decode_bytes_to_waveform(audio_view)
                    num_samples = waveform.shape[-1]
                    audio_duration = num_samples / decoded_sr if decoded_sr > 0 else None
                    # Up-weight sampled durations to estimate total audio processed
                    sample_weight = 1.0 / max(args.rtf_sample_rate, 1e-8)
                except Exception:
                    audio_duration = None
                    sample_weight = 1.0

            tracker.update(audio_duration, duration, sample_weight)

            result = {
                "idx": idx,
                "caption": text,
                "usage": usage,
                "status": "ok",
                "processing_time": duration,
                "retry_attempt": retry_attempt,
                "audio_duration": audio_duration,
                "sample_rate": sr,
                "mime_type": mime,
                "compressed_size": len(audio_view)
            }
            return result
        except Exception as e:
            duration = time.time() - start_time
            return {
                "idx": idx,
                "error": str(e),
                "status": "fail",
                "processing_time": duration,
                "retry_attempt": retry_attempt
            }

    try:
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex, \
             open(args.out_jsonl, "a", encoding="utf-8") as jout, \
             open(args.out_tsv, "a", encoding="utf-8") as tsv:

            futures = {ex.submit(worker, idx): idx for idx in indices}

            for fut in as_completed(futures):
                res = fut.result()

                with lock:
                    if res["status"] == "ok":
                        ok += 1
                        tsv.write(f"{res['idx']}\t{res['caption'].replace(chr(10),' ').strip()}\n")
                    else:
                        fail += 1

                    jout.write(json.dumps(res, ensure_ascii=False) + "\n")

                    # Flush only every 10 writes instead of every write (reduces I/O overhead)
                    write_counter += 1
                    if write_counter % 10 == 0:
                        jout.flush()
                        tsv.flush()

                    processed = ok + fail

                    # Print progress with throughput stats
                    stats = tracker.get_stats()
                    if stats:
                        throughput_str = f"{stats['throughput_samples_per_sec']:.2f} samples/s"
                        if stats.get('real_time_factor'):
                            throughput_str += f" (RTF: {stats['real_time_factor']:.1f}x)"
                        print(f"[{ok}/{total} ok, {fail} fail] idx={res.get('idx')} | {throughput_str}")
                    else:
                        print(f"[{ok}/{total} ok, {fail} fail] idx={res.get('idx')}")

                    # Periodic checkpoint report
                    if processed - last_checkpoint >= args.checkpoint_interval:
                        last_checkpoint = processed
                        print(f"\n{'='*60}")
                        print(f"CHECKPOINT @ {processed}/{total} processed")
                        if stats:
                            print(f"  Elapsed: {stats['elapsed_seconds']:.1f}s")
                            print(f"  Throughput: {stats['throughput_samples_per_sec']:.3f} samples/s")
                            print(f"  Throughput: {stats['throughput_samples_per_hour']:.1f} samples/hour")
                            print(f"  Total audio processed: {stats['total_audio_hours']:.2f} hours")
                            print(f"  Samples with duration: {stats['samples_with_duration']}/{stats['processed']}")

                            if stats.get('real_time_factor'):
                                print(f"  Real-time factor: {stats['real_time_factor']:.1f}x")
                                print(f"  (Processed {stats['total_audio_hours']:.2f}h audio in {stats['elapsed_seconds']/3600:.2f}h wall clock)")

                            # Estimate GPU hours
                            estimates = tracker.estimate_gpu_hours(total_in_range)
                            if estimates:
                                print(f"\n  GPU Hour Estimates (1 GPU):")
                                print(f"    For this range ({total_in_range} samples): {estimates['gpu_hours_for_total']:.2f} GPU hours")
                                if estimates.get('gpu_hours_for_librispeech_960h'):
                                    print(f"    For LibriSpeech 960h: {estimates['gpu_hours_for_librispeech_960h']:.2f} GPU hours")
                        print(f"{'='*60}\n")
    finally:
        close_mmaps()

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
        print(f"  Throughput: {stats['throughput_samples_per_sec']:.3f} samples/s")
        print(f"  Throughput: {stats['throughput_samples_per_hour']:.1f} samples/hour")

        if stats.get('real_time_factor'):
            print(f"\nAudio Processing:")
            print(f"  Total audio processed: {stats['total_audio_hours']:.2f} hours")
            print(f"  Wall clock time: {stats['elapsed_seconds']/3600:.2f} hours")
            print(f"  Real-time factor: {stats['real_time_factor']:.1f}x")
            print(f"  (System processed audio {stats['real_time_factor']:.1f}x faster than real-time)")

        estimates = tracker.estimate_gpu_hours(total_in_range)
        if estimates:
            print(f"\nGPU Hour Estimates (for 1 GPU):")
            print(f"  For this range ({total_in_range} samples): {estimates['gpu_hours_for_total']:.2f} GPU hours")
            if estimates.get('gpu_hours_for_librispeech_960h'):
                print(f"  For LibriSpeech 960h: {estimates['gpu_hours_for_librispeech_960h']:.2f} GPU hours")

    print(f"\nOutput files:")
    print(f"  {args.out_jsonl}")
    print(f"  {args.out_tsv}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
