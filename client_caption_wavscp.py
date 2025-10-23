#!/usr/bin/env python3
import argparse, base64, json, os, shlex, subprocess, sys, time, mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def load_done_set(out_jsonl):
    done = set()
    if not os.path.exists(out_jsonl):
        return done
    with open(out_jsonl, "r", encoding="utf-8") as f:
        for ln in f:
            try:
                obj = json.loads(ln)
                if obj.get("status") == "ok":
                    done.add(obj.get("utt"))
            except Exception:
                pass
    return done

def main():
    args = parse_args()
    os.makedirs(os.path.dirname(args.out_jsonl) or ".", exist_ok=True)
    done = load_done_set(args.out_jsonl) if args.resume else set()

    entries = [(u, s) for u, s in read_wavscp(args.scp) if (u not in done)]
    total = len(entries)
    print(f"Total to process: {total}")

    session = requests.Session()
    ok = 0
    fail = 0

    def worker(pair):
        utt, src = pair
        try:
            data, mime = load_audio_bytes(src)
            data_url = to_data_url(data, mime)
            text, usage = post_one(session, args.base_url, args.model, data_url, args.timeout)
            return {"utt": utt, "caption": text, "usage": usage, "status": "ok"}
        except Exception as e:
            return {"utt": utt, "error": str(e), "status": "fail"}

    with ThreadPoolExecutor(max_workers=args.max_workers) as ex, \
         open(args.out_jsonl, "a", encoding="utf-8") as jout, \
         open(args.out_tsv, "a", encoding="utf-8") as tsv:
        futures = [ex.submit(worker, p) for p in entries]
        for fut in as_completed(futures):
            res = fut.result()
            if res["status"] == "ok":
                ok += 1
                tsv.write(f"{res['utt']}\t{res['caption'].replace('\\n',' ').strip()}\n")
                tsv.flush()
            else:
                fail += 1
            jout.write(json.dumps(res, ensure_ascii=False) + "\n")
            jout.flush()
            print(f"[{ok}/{total} ok, {fail} fail] {res.get('utt')}")

    print(f"Done. Wrote:\n  {args.out_jsonl}\n  {args.out_tsv}")

if __name__ == "__main__":
    main()

