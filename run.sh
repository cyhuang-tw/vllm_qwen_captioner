#!/usr/bin/env bash
set -euo pipefail

# ==== user config ====
SIF_IMG="/work/hdd/bbjs/chuang14/apptainer/vllm_v2.sif"   # container image path
AUDIO_SCP="$PWD/wav.scp"                   # input wav.scp file
OUT_DIR="$PWD/out"
PORT=8901
TP_SIZE=1
MAX_WORKERS=2000        # Large number to keep vLLM queue full
MAX_QUEUE_SIZE=999999   # Effectively disable throttling

mkdir -p "$OUT_DIR" logs

JOBID=$(sbatch --parsable \
  --export=ALL,SIF_IMG="$SIF_IMG",AUDIO_SCP="$AUDIO_SCP",OUT_DIR="$OUT_DIR",PORT="$PORT",TP_SIZE="$TP_SIZE",MAX_WORKERS="$MAX_WORKERS",MAX_QUEUE_SIZE="$MAX_QUEUE_SIZE" \
  qwen_caption_job.sbatch)

if [[ -n "$JOBID" ]]; then
  echo "Submitted job $JOBID"
  echo "Logs → logs/qwen-cap-${JOBID}.out"
else
  echo "Job submission failed."
fi

