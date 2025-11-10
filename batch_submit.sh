#!/usr/bin/env bash
#
# batch_submit.sh - Split SCP file and submit multiple parallel processing jobs
#
# Usage:
#   ./batch_submit.sh INPUT_SCP NUM_JOBS [OPTIONS]
#
# Example:
#   ./batch_submit.sh large_wav.scp 10 --out-dir ./results
#
# Arguments:
#   INPUT_SCP     - Path to input SCP file
#   NUM_JOBS      - Number of parallel jobs to create
#
# Options:
#   --out-dir DIR        - Output directory (default: ./out)
#   --sif-img PATH       - Apptainer image path (default: /work/hdd/bbjs/chuang14/apptainer/vllm_v2.sif)
#   --tp-size N          - Tensor parallel size (default: 1)
#   --max-workers N      - Max concurrent workers per job (default: 2000)
#   --max-queue-size N   - Max vLLM queue size (default: 999999)
#   --port-start N       - Starting port number (default: 8901)
#   --partition NAME     - SLURM partition (default: gpuH200x8-interactive)
#   --account NAME       - SLURM account (default: bbjs-delta-gpu)
#   --time HH:MM:SS      - Time limit per job (default: 1:00:00)
#   --mem SIZE           - Memory per job (default: 96G)
#   --cpus N             - CPUs per job (default: 8)
#   --dry-run            - Show what would be submitted without actually submitting
#   --help               - Show this help message

set -euo pipefail

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# Logging functions (output to stderr to avoid capturing in command substitution)
info() { echo -e "${BLUE}[INFO]${NC} $*" >&2; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $*" >&2; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*" >&2; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
bold() { echo -e "${BOLD}$*${NC}" >&2; }

# Show help
show_help() {
    head -n 25 "$0" | grep '^#' | sed 's/^# \?//'
    exit 0
}

# Function to split SCP file into N chunks
split_scp_file() {
    local input_scp="$1"
    local num_chunks="$2"
    local output_dir="$3"

    info "Splitting SCP file into $num_chunks chunks..."
    info "Input: $input_scp"

    # Count valid lines (skip comments and empty lines)
    local total_lines=$(grep -v '^#' "$input_scp" | grep -v '^[[:space:]]*$' | wc -l)
    info "Total audio files: $total_lines"

    if [ "$total_lines" -eq 0 ]; then
        error "No valid lines found in $input_scp"
        return 1
    fi

    if [ "$num_chunks" -gt "$total_lines" ]; then
        warn "NUM_JOBS ($num_chunks) is greater than total lines ($total_lines)"
        warn "Adjusting to $total_lines jobs"
        num_chunks=$total_lines
    fi

    # Calculate lines per chunk
    local lines_per_chunk=$(( (total_lines + num_chunks - 1) / num_chunks ))
    info "Lines per chunk: ~$lines_per_chunk"

    # Create output directory
    mkdir -p "$output_dir"

    # Get base name
    local base_name=$(basename "$input_scp" .scp)

    # Create temporary filtered file (no comments/empty lines)
    local temp_file=$(mktemp)
    # Ensure temp file is cleaned up on exit/error
    trap "rm -f '$temp_file'" RETURN

    grep -v '^#' "$input_scp" | grep -v '^[[:space:]]*$' > "$temp_file"

    # Save current directory for later return
    local original_dir="$PWD"

    # Split into chunks
    cd "$output_dir" || {
        error "Failed to change to output directory: $output_dir"
        return 1
    }
    split -l "$lines_per_chunk" -d -a 4 "$temp_file" "${base_name}_chunk_"

    # Add .scp extension and header to each chunk
    local chunk_count=0
    # Use array to safely handle filenames with spaces
    # Save nullglob state and enable it temporarily
    local nullglob_was_set=false
    if shopt -q nullglob; then
        nullglob_was_set=true
    else
        shopt -s nullglob
    fi

    local chunk_files=("${base_name}_chunk_"*)

    # Restore nullglob state
    if [ "$nullglob_was_set" = false ]; then
        shopt -u nullglob
    fi

    for chunk_file in "${chunk_files[@]}"; do
        # Skip if not a file or already has .scp extension
        if [ ! -f "$chunk_file" ]; then
            continue
        fi

        # Skip files that already have .scp extension
        case "$chunk_file" in
            *.scp)
                continue
                ;;
        esac

        local scp_file="${chunk_file}.scp"
        local line_count=$(wc -l < "$chunk_file")

        # Create SCP file with header
        {
            echo "# Auto-generated chunk $chunk_count from $input_scp"
            echo "# Generated at: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "# Total lines: $line_count"
            echo ""
            cat "$chunk_file"
        } > "$scp_file"

        rm "$chunk_file"
        success "Created chunk $chunk_count: $scp_file ($line_count files)"
        chunk_count=$((chunk_count + 1))
    done

    # Return to original directory
    cd "$original_dir" || true  # Don't fail if we can't return

    success "Split complete! Created $chunk_count SCP chunks in $output_dir/"
    echo "$chunk_count"
    # Note: temp file cleanup handled by trap
}

# Default configuration
OUT_DIR="./out"
SIF_IMG="/work/hdd/bbjs/chuang14/apptainer/vllm_v2.sif"
TP_SIZE=1
MAX_WORKERS=2000
MAX_QUEUE_SIZE=999999
PORT_START=8901
PARTITION="gpuH200x8"
ACCOUNT="bbjs-delta-gpu"
TIME_LIMIT="24:00:00"
MEMORY="128G"
CPUS=8
DRY_RUN=false

# Parse arguments
if [ $# -lt 2 ]; then
    show_help
fi

INPUT_SCP="$1"
NUM_JOBS="$2"
shift 2

# Parse options
while [ $# -gt 0 ]; do
    case "$1" in
        --out-dir)
            OUT_DIR="$2"
            shift 2
            ;;
        --sif-img)
            SIF_IMG="$2"
            shift 2
            ;;
        --tp-size)
            TP_SIZE="$2"
            shift 2
            ;;
        --max-workers)
            MAX_WORKERS="$2"
            shift 2
            ;;
        --max-queue-size)
            MAX_QUEUE_SIZE="$2"
            shift 2
            ;;
        --port-start)
            PORT_START="$2"
            shift 2
            ;;
        --partition)
            PARTITION="$2"
            shift 2
            ;;
        --account)
            ACCOUNT="$2"
            shift 2
            ;;
        --time)
            TIME_LIMIT="$2"
            shift 2
            ;;
        --mem)
            MEMORY="$2"
            shift 2
            ;;
        --cpus)
            CPUS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help)
            show_help
            ;;
        *)
            error "Unknown option: $1"
            show_help
            ;;
    esac
done

# Validate inputs
if [ ! -f "$INPUT_SCP" ]; then
    error "Input SCP file not found: $INPUT_SCP"
    exit 1
fi

if ! [[ "$NUM_JOBS" =~ ^[0-9]+$ ]] || [ "$NUM_JOBS" -lt 1 ]; then
    error "NUM_JOBS must be a positive integer"
    exit 1
fi

# Check required files
SBATCH_FILE="${SCRIPT_DIR}/process_chunk.sbatch"
CLIENT_SCRIPT="${SCRIPT_DIR}/client_caption_wavscp.py"

if [ ! -f "$SBATCH_FILE" ]; then
    error "SBATCH script not found: $SBATCH_FILE"
    error "Make sure process_chunk.sbatch exists in the same directory"
    exit 1
fi

if [ ! -f "$CLIENT_SCRIPT" ]; then
    error "Client script not found: $CLIENT_SCRIPT"
    error "Make sure client_caption_wavscp.py exists in the same directory"
    exit 1
fi

if [ ! -f "$SIF_IMG" ]; then
    warn "Container image not found: $SIF_IMG"
    warn "Jobs may fail if this path is incorrect!"
fi

# Convert to absolute paths
INPUT_SCP="$(realpath "$INPUT_SCP")"
OUT_DIR="$(realpath -m "$OUT_DIR")"

# Print configuration
echo "" >&2
bold "=========================================="
bold "    Batch Processing Configuration"
bold "=========================================="
info "Input SCP:       $INPUT_SCP"
info "Number of jobs:  $NUM_JOBS"
info "Output dir:      $OUT_DIR"
info "Container:       $SIF_IMG"
info "Tensor parallel: $TP_SIZE"
info "Max workers:     $MAX_WORKERS"
info "Port range:      $PORT_START - $((PORT_START + NUM_JOBS - 1))"
info "Partition:       $PARTITION"
info "Account:         $ACCOUNT"
info "Time limit:      $TIME_LIMIT"
info "Memory:          $MEMORY"
info "CPUs per job:    $CPUS"
[ "$DRY_RUN" = true ] && warn "DRY RUN MODE - No jobs will be submitted"
bold "=========================================="
echo "" >&2

# Create directories
SPLIT_DIR="${OUT_DIR}/scp_splits"
mkdir -p "$SPLIT_DIR"
mkdir -p "${SCRIPT_DIR}/logs"

# Split the SCP file
if ! actual_chunks=$(split_scp_file "$INPUT_SCP" "$NUM_JOBS" "$SPLIT_DIR"); then
    error "Failed to split SCP file"
    exit 1
fi

if [ -z "$actual_chunks" ] || [ "$actual_chunks" -eq 0 ]; then
    error "Failed to create any chunks"
    exit 1
fi

echo "" >&2
info "Submitting SLURM jobs..."
echo "" >&2

# Submit jobs
job_ids=()
chunk_num=0

for scp_chunk in "${SPLIT_DIR}"/*.scp; do
    if [ ! -f "$scp_chunk" ]; then
        continue
    fi

    # Calculate unique port
    port=$((PORT_START + chunk_num))

    # Create output directory for this chunk
    chunk_out_dir="${OUT_DIR}/chunk_${chunk_num}"
    mkdir -p "$chunk_out_dir"

    # Stable run ID per chunk so resume reuses the same outputs
    chunk_base="$(basename "$scp_chunk" .scp)"
    run_id="$chunk_base"

    # Get absolute path for SCP chunk
    scp_chunk="$(realpath "$scp_chunk")"

    info "Chunk $((chunk_num + 1))/$actual_chunks:"
    info "  SCP:    $(basename "$scp_chunk")"
    info "  Port:   $port"
    info "  Output: $chunk_out_dir"

    if [ "$DRY_RUN" = true ]; then
        # Show what would be submitted
        warn "[DRY RUN] Would submit job with:"
        warn "  Job name: vllm-cap-${chunk_num}"
        warn "  SCP: $scp_chunk"
        warn "  Port: $port"
        warn "  Output: $chunk_out_dir"
        job_ids+=("DRY_${chunk_num}")
    else
        # Submit the job - use environment variables to avoid quoting issues in --export
        # Set variables in a subshell to avoid polluting current environment
        job_output=$(
            export SIF_IMG="$SIF_IMG"
            export AUDIO_SCP="$scp_chunk"
            export OUT_DIR="$chunk_out_dir"
            export PORT="$port"
            export TP_SIZE="$TP_SIZE"
            export MAX_WORKERS="$MAX_WORKERS"
            export MAX_QUEUE_SIZE="$MAX_QUEUE_SIZE"
            export RUN_ID="$run_id"

            sbatch \
                -J "vllm-cap-${chunk_num}" \
                -p "$PARTITION" \
                --account="$ACCOUNT" \
                --gres=gpu:1 \
                -c "$CPUS" \
                --mem="$MEMORY" \
                -t "$TIME_LIMIT" \
                -o "${SCRIPT_DIR}/logs/vllm-cap-${chunk_num}-%j.out" \
                --chdir="$SCRIPT_DIR" \
                --export=ALL \
                "$SBATCH_FILE" 2>&1
        )

        # Extract job ID (more robust extraction)
        if echo "$job_output" | grep -q "Submitted batch job"; then
            # Extract the number after "Submitted batch job"
            job_id=$(echo "$job_output" | sed -n 's/^Submitted batch job \([0-9][0-9]*\).*$/\1/p')
            if [ -z "$job_id" ]; then
                error "  → Failed to extract job ID from: $job_output"
                exit 1
            fi
            job_ids+=("$job_id")
            success "  → Submitted job $job_id"
        else
            error "  → Failed to submit job: $job_output"
            exit 1
        fi
    fi

    chunk_num=$((chunk_num + 1))
    echo "" >&2
done

# Print summary
echo "" >&2
bold "=========================================="
bold "       Submission Complete!"
bold "=========================================="
success "Jobs submitted: ${#job_ids[@]}"

if [ "$DRY_RUN" = false ]; then
    success "Job IDs: ${job_ids[*]}"
    echo "" >&2
    info "Monitor your jobs:"
    info "  squeue -u \$USER"
    info "  squeue -j $(echo ${job_ids[*]} | tr ' ' ',')"
    echo "" >&2
    info "View logs:"
    info "  tail -f ${SCRIPT_DIR}/logs/vllm-cap-*-*.out"
    echo "" >&2
    info "Cancel all jobs:"
    info "  scancel ${job_ids[*]}"
    echo "" >&2
    info "Output locations:"
    for i in "${!job_ids[@]}"; do
        info "  Job ${job_ids[$i]}: ${OUT_DIR}/chunk_${i}/"
    done
    echo "" >&2
    info "After completion, merge results:"
    info "  python3 merge_jsonl.py ${OUT_DIR}/chunk_*/captions-*.jsonl \\"
    info "    --output ${OUT_DIR}/merged.jsonl \\"
    info "    --output-tsv ${OUT_DIR}/merged.tsv"
fi
bold "=========================================="
