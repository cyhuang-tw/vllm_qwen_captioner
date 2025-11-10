#!/usr/bin/env bash
#
# batch_submit_arkive.sh - Split arkive index range and submit parallel jobs
#
# Usage:
#   ./batch_submit_arkive.sh ARKDIR NUM_JOBS [OPTIONS]
#
# Example:
#   ./batch_submit_arkive.sh /path/to/arkive 8 --out-dir ./out_arkive
#
# Options:
#   --out-dir DIR      - Output directory (default: ./out_arkive)
#   --sif-img PATH     - Apptainer image path (default: /work/hdd/bbjs/chuang14/apptainer/vllm_v2.sif)
#   --tp-size N        - Tensor parallel size (default: 1)
#   --max-workers N    - Max concurrent workers per job (default: 2000)
#   --max-tokens N     - Max new tokens per caption (default: 200)
#   --temperature F    - Sampling temperature (default: 0.2)
#   --rtf-sample-rate F- Probability [0,1] of decoding for RTF (default: 0)
#   --port-start N     - Starting port number (default: 8801)
#   --partition NAME   - SLURM partition (default: gpuH200x8)
#   --account NAME     - SLURM account (default: bbjs-delta-gpu)
#   --time HH:MM:SS    - Time limit per job (default: 24:00:00)
#   --mem SIZE         - Memory per job (default: 128G)
#   --cpus N           - CPUs per job (default: 8)
#   --dry-run          - Show what would be submitted without actually submitting
#   --help             - Show this help message

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

info() { echo -e "${BLUE}[INFO]${NC} $*" >&2; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $*" >&2; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*" >&2; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
bold() { echo -e "${BOLD}$*${NC}" >&2; }

show_help() {
    head -n 40 "$0" | grep '^#' | sed 's/^# \?//'
    exit 0
}

if [ $# -lt 2 ]; then
    show_help
fi

ARKDIR="$1"
NUM_JOBS="$2"
shift 2

OUT_DIR="./out_arkive"
SIF_IMG="/work/hdd/bbjs/chuang14/apptainer/vllm_v2.sif"
TP_SIZE=1
MAX_WORKERS=2000
MAX_TOKENS=200
TEMPERATURE=0.2
RTF_SAMPLE_RATE=0
PORT_START=8801
PARTITION="gpuH200x8-interactive"
ACCOUNT="bbjs-delta-gpu"
TIME_LIMIT="1:00:00"
MEMORY="128G"
CPUS=8
DRY_RUN=false

while [ $# -gt 0 ]; do
    case "$1" in
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        --sif-img) SIF_IMG="$2"; shift 2 ;;
        --tp-size) TP_SIZE="$2"; shift 2 ;;
        --max-workers) MAX_WORKERS="$2"; shift 2 ;;
        --max-tokens) MAX_TOKENS="$2"; shift 2 ;;
        --temperature) TEMPERATURE="$2"; shift 2 ;;
        --rtf-sample-rate) RTF_SAMPLE_RATE="$2"; shift 2 ;;
        --port-start) PORT_START="$2"; shift 2 ;;
        --partition) PARTITION="$2"; shift 2 ;;
        --account) ACCOUNT="$2"; shift 2 ;;
        --time) TIME_LIMIT="$2"; shift 2 ;;
        --mem) MEMORY="$2"; shift 2 ;;
        --cpus) CPUS="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        --help) show_help ;;
        *) error "Unknown option: $1"; show_help ;;
    esac
done

# Validate
PARQUET_PATH="${ARKDIR}/metadata.parquet"
if [ ! -f "$PARQUET_PATH" ]; then
    error "metadata.parquet not found at $PARQUET_PATH"
    exit 1
fi

if ! [[ "$NUM_JOBS" =~ ^[0-9]+$ ]] || [ "$NUM_JOBS" -lt 1 ]; then
    error "NUM_JOBS must be a positive integer"
    exit 1
fi

if [ ! -f "$SIF_IMG" ]; then
    warn "Container image not found: $SIF_IMG"
fi

# Resolve paths
ARKDIR="$(realpath "$ARKDIR")"
OUT_DIR="$(realpath -m "$OUT_DIR")"

# Count total items via pyarrow
total_items=$(python3 - <<PY
import sys
try:
    import pyarrow.parquet as pq
except Exception as e:
    sys.stderr.write(f"ERROR: pyarrow not available: {e}\\n")
    sys.exit(1)
pf = pq.ParquetFile("${PARQUET_PATH}")
print(pf.metadata.num_rows)
PY
)

if ! [[ "$total_items" =~ ^[0-9]+$ ]]; then
    error "Failed to read total items from metadata.parquet"
    exit 1
fi

if [ "$NUM_JOBS" -gt "$total_items" ]; then
    warn "NUM_JOBS ($NUM_JOBS) > total items ($total_items); reducing to $total_items"
    NUM_JOBS=$total_items
fi

items_per_job=$(( (total_items + NUM_JOBS - 1) / NUM_JOBS ))

echo "" >&2
bold "=============================================="
bold "          Arkive Batch Configuration"
bold "=============================================="
info "Arkive dir:     $ARKDIR"
info "Total items:    $total_items"
info "Jobs:           $NUM_JOBS"
info "Items/job:      ~$items_per_job"
info "Out dir:        $OUT_DIR"
info "Container:      $SIF_IMG"
info "TP size:        $TP_SIZE"
info "Max workers:    $MAX_WORKERS"
info "Max tokens:     $MAX_TOKENS"
info "Temperature:    $TEMPERATURE"
info "RTF sample:     $RTF_SAMPLE_RATE"
info "Port range:     $PORT_START - $((PORT_START + NUM_JOBS - 1))"
[ "$DRY_RUN" = true ] && warn "DRY RUN MODE - no jobs will be submitted"
bold "=============================================="
echo "" >&2

mkdir -p "$OUT_DIR"
mkdir -p "${SCRIPT_DIR}/logs"

# Submit jobs
job_ids=()
for ((i=0; i<NUM_JOBS; i++)); do
    start_idx=$(( i * items_per_job ))
    end_idx=$(( (i + 1) * items_per_job ))
    if [ "$start_idx" -ge "$total_items" ]; then
        continue
    fi
    if [ "$end_idx" -gt "$total_items" ]; then
        end_idx=$total_items
    fi

    port=$((PORT_START + i))
    chunk_out_dir="${OUT_DIR}/chunk_${i}"
    mkdir -p "$chunk_out_dir"

    run_id="ark_chunk_${i}_${start_idx}_${end_idx}"

    info "Chunk $((i+1))/$NUM_JOBS: idx [$start_idx, $end_idx) port $port run_id $run_id"

    if [ "$DRY_RUN" = true ]; then
        warn "[DRY RUN] Would submit job for range [$start_idx,$end_idx)"
        job_ids+=("DRY_${i}")
        continue
    fi

    job_output=$(
        export SIF_IMG="$SIF_IMG"
        export ARKDIR="$ARKDIR"
        export OUT_DIR="$chunk_out_dir"
        export PORT="$port"
        export TP_SIZE="$TP_SIZE"
        export MAX_WORKERS="$MAX_WORKERS"
        export MAX_TOKENS="$MAX_TOKENS"
        export TEMPERATURE="$TEMPERATURE"
        export RTF_SAMPLE_RATE="$RTF_SAMPLE_RATE"
        export START_IDX="$start_idx"
        export END_IDX="$end_idx"
        export RUN_ID="$run_id"

        sbatch \
            -J "vllm-ark-${i}" \
            -p "$PARTITION" \
            --account="$ACCOUNT" \
            --gres=gpu:1 \
            -c "$CPUS" \
            --mem="$MEMORY" \
            -t "$TIME_LIMIT" \
            -o "${SCRIPT_DIR}/logs/vllm-ark-${i}-%j.out" \
            --chdir="$SCRIPT_DIR" \
            --export=ALL \
            "${SCRIPT_DIR}/process_chunk_arkive.sbatch" 2>&1
    )

    if echo "$job_output" | grep -q "Submitted batch job"; then
        job_id=$(echo "$job_output" | sed -n 's/^Submitted batch job \([0-9][0-9]*\).*$/\1/p')
        if [ -z "$job_id" ]; then
            error "Failed to extract job ID from: $job_output"
            exit 1
        fi
        job_ids+=("$job_id")
        success "  → Submitted job $job_id"
    else
        error "Failed to submit job: $job_output"
        exit 1
    fi
done

echo "" >&2
bold "=============================================="
bold "           Submission Complete"
bold "=============================================="
success "Jobs submitted: ${#job_ids[@]}"
if [ "$DRY_RUN" = false ]; then
    success "Job IDs: ${job_ids[*]}"
    echo "" >&2
    info "Monitor:   squeue -u \$USER"
    info "Logs:      tail -f ${SCRIPT_DIR}/logs/vllm-ark-*-*.out"
    info "Cancel:    scancel ${job_ids[*]}"
    echo "" >&2
    info "Output per chunk:"
    for i in "${!job_ids[@]}"; do
        info "  chunk_${i}: ${OUT_DIR}/chunk_${i}/"
    done
fi
bold "=============================================="
