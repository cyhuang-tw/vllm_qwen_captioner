# Batch Processing Guide

Process large SCP files in parallel by automatically splitting them into chunks and submitting multiple SLURM jobs.

## Quick Start

```bash
# Process large_wav.scp with 10 parallel jobs
./batch_submit.sh large_wav.scp 10
```

That's it! The script will:
1. Split your SCP file into 10 equal chunks
2. Submit 10 SLURM jobs (each runs its own server + client)
3. Save outputs to `./out/chunk_0/`, `./out/chunk_1/`, etc.

## Basic Usage

```bash
./batch_submit.sh INPUT_SCP NUM_JOBS [OPTIONS]
```

### Required Arguments
- `INPUT_SCP` - Path to your SCP file
- `NUM_JOBS` - Number of parallel jobs (chunks to create)

### Common Options
- `--out-dir DIR` - Output directory (default: `./out`)
- `--time HH:MM:SS` - Time limit per job (default: `1:00:00`)
- `--max-workers N` - Concurrent workers per job (default: `2000`)
- `--dry-run` - Preview without submitting
- `--help` - Show all options

## Examples

### Example 1: Basic Usage
```bash
./batch_submit.sh large_wav.scp 10
```

### Example 2: Custom Output Directory
```bash
./batch_submit.sh large_wav.scp 20 --out-dir /work/nvme/bbjs/chuang14/results
```

### Example 3: Longer Time Limit
```bash
./batch_submit.sh large_wav.scp 10 --time 4:00:00
```

### Example 4: Preview Without Submitting
```bash
./batch_submit.sh large_wav.scp 10 --dry-run
```

### Example 5: Custom Resources
```bash
./batch_submit.sh large_wav.scp 15 \
    --time 2:00:00 \
    --mem 128G \
    --cpus 16 \
    --max-workers 3000
```

### Example 6: Different Partition
```bash
./batch_submit.sh large_wav.scp 10 \
    --partition gpuA100x8-interactive \
    --account my-project-gpu
```

## How It Works

### Architecture

Each job is completely independent:
```
Job 1 (Port 8901)          Job 2 (Port 8902)          Job N (Port 890N)
├─ vLLM Server             ├─ vLLM Server             ├─ vLLM Server
├─ Client                  ├─ Client                  ├─ Client
└─ chunk_0.scp             └─ chunk_1.scp             └─ chunk_N.scp
```

### Workflow

1. **Splitting Phase**
   - Reads your SCP file
   - Filters out comments and empty lines
   - Divides into N equal chunks
   - Saves to `out/scp_splits/`

2. **Submission Phase**
   - Submits N SLURM jobs
   - Each job gets unique port (8901, 8902, ...)
   - Each job has its own output directory

3. **Execution Phase** (per job)
   - Install dependencies
   - Start vLLM server
   - Wait for server ready
   - Run caption client
   - Cleanup server on exit

4. **Output Organization**
   ```
   out/
   ├── scp_splits/
   │   ├── large_wav_chunk_0000.scp
   │   ├── large_wav_chunk_0001.scp
   │   └── ...
   ├── chunk_0/
   │   ├── captions-JOBID.jsonl
   │   └── captions-JOBID.tsv
   ├── chunk_1/
   │   ├── captions-JOBID.jsonl
   │   └── captions-JOBID.tsv
   └── ...
   ```

## Monitoring Jobs

After submission, you'll see job IDs:

```bash
# Check all your jobs
squeue -u $USER

# Check specific jobs (use IDs from output)
squeue -j 12345,12346,12347

# View logs in real-time
tail -f logs/vllm-cap-*-*.out

# Check a specific job's log
tail -f logs/vllm-cap-0-12345.out
```

## Merging Results

After all jobs complete:

```bash
# Merge all JSONL files
python3 merge_jsonl.py out/chunk_*/captions-*.jsonl \
    --output out/merged.jsonl \
    --output-tsv out/merged.tsv

# Or use the batch merge script
./merge_all_outputs.sh out/
```

The merge script will:
- Combine all JSONL files
- Remove duplicates (keeps first occurrence)
- Create both JSONL and TSV outputs

## Choosing NUM_JOBS

### Quick Guide
- **Small** (< 1,000 files): 2-5 jobs
- **Medium** (1,000-10,000 files): 5-20 jobs
- **Large** (10,000-100,000 files): 20-50 jobs
- **Very large** (> 100,000 files): 50-100 jobs

### Considerations
1. Each job needs 1 GPU
2. Check GPU availability: `sinfo -p gpuH200x8-interactive`
3. More jobs = faster but more resources
4. Each job has startup overhead (~1-2 minutes)

### Speed Estimate
If each audio takes ~2 seconds to process:
- **1 job**: 10,000 files = ~5.5 hours
- **10 jobs**: 10,000 files = ~33 minutes
- **20 jobs**: 10,000 files = ~17 minutes

## All Configuration Options

```bash
./batch_submit.sh INPUT_SCP NUM_JOBS \
    --out-dir DIR              # Output directory
    --sif-img PATH             # Container image path
    --tp-size N                # Tensor parallel size (for multi-GPU)
    --max-workers N            # Concurrent requests per job
    --max-queue-size N         # vLLM queue threshold
    --port-start N             # Starting port (8901, 8902, ...)
    --partition NAME           # SLURM partition
    --account NAME             # SLURM account
    --time HH:MM:SS            # Time limit per job
    --mem SIZE                 # Memory per job (e.g., 96G)
    --cpus N                   # CPUs per job
    --dry-run                  # Preview only
    --help                     # Show help
```

## Troubleshooting

### Problem: Jobs fail immediately
**Check logs:**
```bash
cat logs/vllm-cap-*-*.out
```

**Common causes:**
- SCP file path incorrect
- Container image not found
- No GPU available

### Problem: Port conflicts
**Solution:** Use different starting port
```bash
./batch_submit.sh large_wav.scp 10 --port-start 9000
```

### Problem: Out of GPU quota
**Solution:** Reduce NUM_JOBS or check quota
```bash
# Check your quota
sacctmgr show assoc user=$USER format=user,account,grptres

# Use fewer jobs
./batch_submit.sh large_wav.scp 5
```

### Problem: Jobs timeout
**Solution:** Increase time limit
```bash
./batch_submit.sh large_wav.scp 10 --time 4:00:00
```

### Problem: Need to cancel jobs
**Use scancel (command provided in output):**
```bash
scancel 12345 12346 12347 ...
```

### Problem: Job failed mid-processing
**Solution:** Just resubmit! The client has resume capability:
```bash
# Failed jobs will skip already-processed files
./batch_submit.sh large_wav.scp 10  # Same command works!
```

## Advanced Usage

### Multi-GPU Processing
Use tensor parallelism for larger models:
```bash
./batch_submit.sh large_wav.scp 5 \
    --tp-size 2 \
    --partition gpuH200x8 \
    --cpus 16
```

### Custom Container
```bash
./batch_submit.sh large_wav.scp 10 \
    --sif-img /path/to/custom/container.sif
```

### Different Queue Settings
```bash
./batch_submit.sh large_wav.scp 10 \
    --max-workers 5000 \
    --max-queue-size 4000
```

## Complete Workflow Example

```bash
# 1. Check your SCP file
wc -l large_wav.scp
# Output: 50000 lines

# 2. Test with dry run
./batch_submit.sh large_wav.scp 25 --dry-run

# 3. Submit for real
./batch_submit.sh large_wav.scp 25 --out-dir results_$(date +%Y%m%d)
# Output shows job IDs: 12345 12346 12347 ...

# 4. Monitor progress
squeue -u $USER
watch -n 5 'squeue -u $USER'  # Auto-refresh every 5 seconds

# 5. Check logs
tail -f logs/vllm-cap-*-*.out

# 6. After completion, merge results
python3 merge_jsonl.py results_*/chunk_*/captions-*.jsonl \
    --output results_merged.jsonl \
    --output-tsv results_merged.tsv

# 7. Verify output
wc -l results_merged.jsonl
head results_merged.tsv
```

## Performance Tips

1. **Start small**: Test with 2-3 jobs first
2. **Use dry-run**: Always preview first
3. **Monitor resources**: Check GPU/memory usage
4. **Choose appropriate NUM_JOBS**: Balance speed vs resources
5. **Use resume**: Don't worry about failures - just resubmit

## Files Created

- `batch_submit.sh` - Main orchestration script
- `process_chunk.sbatch` - SBATCH template (runs server + client)
- `logs/` - Job output logs
- `out/scp_splits/` - Split SCP files
- `out/chunk_*/` - Per-job outputs

## Getting Help

```bash
# Show all options
./batch_submit.sh --help

# Check SLURM job details
scontrol show job JOBID

# View full job output
cat logs/vllm-cap-0-JOBID.out
```

## Notes

- Each job is completely independent
- Jobs can fail/restart without affecting others
- Resume capability handles interrupted processing
- Automatic cleanup stops servers on exit
- Supports comments in SCP files (lines starting with `#`)
