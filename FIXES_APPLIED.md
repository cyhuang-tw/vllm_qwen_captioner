# Issues Found and Fixed

## Summary
After careful review, I found and fixed **2 critical bugs** that would have caused the scripts to fail.

---

## Issue 1: Command Substitution Capturing Wrong Output ⚠️ CRITICAL

**File:** `batch_submit.sh` (line 279-293)

**Problem:**
The logging functions (info, success, warn, error) were outputting to stdout. When capturing the output of `split_scp_file()` with command substitution:
```bash
actual_chunks=$(split_scp_file "$INPUT_SCP" "$NUM_JOBS" "$SPLIT_DIR")
```
We were capturing ALL stdout output (including log messages), not just the final chunk count.

This caused the check `[ "$actual_chunks" -eq 0 ]` to fail with "integer expression expected" because `$actual_chunks` contained multi-line text instead of a number.

**Fix:**
Changed all logging functions to output to stderr (>&2):
```bash
info() { echo -e "${BLUE}[INFO]${NC} $*" >&2; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $*" >&2; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*" >&2; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
bold() { echo -e "${BOLD}$*${NC}" >&2; }
```

Now command substitution only captures the final `echo "$chunk_count"` which outputs to stdout.

**Impact:** Without this fix, the script would crash with "integer expression expected" error.

---

## Issue 2: Unreachable Error Handling Code ⚠️ CRITICAL

**File:** `process_chunk.sbatch` (line 150-178)

**Problem:**
The script uses `set -euo pipefail` which causes immediate exit on any command failure. When the Python client command failed, the script would exit immediately, and the error handling code (lines 175-177) was unreachable:

```bash
python3 -u client_caption_wavscp.py ...
EXIT_CODE=$?  # Never reached if python3 fails

if [ $EXIT_CODE -eq 0 ]; then
    # success message
else
    # This was unreachable!
    echo "ERROR: Client exited with code $EXIT_CODE"
    exit $EXIT_CODE
fi
```

**Fix:**
Temporarily disable `set -e` around the Python command to capture the exit code:
```bash
set +e  # Temporarily disable exit-on-error
python3 -u client_caption_wavscp.py ...
EXIT_CODE=$?
set -e  # Re-enable exit-on-error
```

Also improved the error message to be more helpful.

**Impact:** Without this fix, failures would exit immediately without the nice error message, and the error handling logic was dead code.

---

## Additional Improvements Made

### 3. Job ID Extraction Robustness

**File:** `batch_submit.sh` (line 337-348)

**Changed from:**
```bash
job_id=$(echo "$job_output" | grep -o '[0-9]\+' | head -n1)
```

**Changed to:**
```bash
job_id=$(echo "$job_output" | sed -n 's/^Submitted batch job \([0-9][0-9]*\).*$/\1/p')
if [ -z "$job_id" ]; then
    error "  → Failed to extract job ID from: $job_output"
    exit 1
fi
```

**Reason:** The `grep -o '[0-9]\+'` pattern uses extended regex which isn't portable. Using `sed` is more portable and the pattern is more specific (extracts number after "Submitted batch job").

---

### 4. File Extension Check Clarity

**File:** `batch_submit.sh` (line 100-128)

**Changed from:**
```bash
if [ -f "$chunk_file" ] && [ ! "${chunk_file}" = "${chunk_file%.scp}" ]; then
    continue  # Skip if already has .scp extension
fi
```

**Changed to:**
```bash
case "$chunk_file" in
    *.scp)
        continue
        ;;
esac
```

**Reason:** The case statement is clearer and more idiomatic for pattern matching.

---

### 5. Error Handling for split_scp_file

**File:** `batch_submit.sh` (line 279-293)

**Improved validation:**
```bash
if ! actual_chunks=$(split_scp_file "$INPUT_SCP" "$NUM_JOBS" "$SPLIT_DIR"); then
    error "Failed to split SCP file"
    exit 1
fi

if [ -z "$actual_chunks" ] || [ "$actual_chunks" -eq 0 ]; then
    error "Failed to create any chunks"
    exit 1
fi
```

**Reason:** Better error handling to catch failures from the split function and validate the result.

---

## Testing Recommendations

Before running on production data:

1. **Test with dry-run:**
   ```bash
   ./batch_submit.sh test_audio.scp 2 --dry-run
   ```

2. **Test with small SCP file:**
   ```bash
   ./batch_submit.sh test_audio.scp 2 --out-dir ./test_out
   ```

3. **Verify chunk files created:**
   ```bash
   ls -la test_out/scp_splits/
   ```

4. **Monitor job logs:**
   ```bash
   tail -f logs/vllm-cap-*-*.out
   ```

---

## All Issues Resolved ✅

Both scripts are now production-ready with:
- ✅ Correct stdout/stderr handling
- ✅ Proper error handling and reporting
- ✅ Portable regex patterns
- ✅ Clear and maintainable code
- ✅ Robust validation

No further issues found.
