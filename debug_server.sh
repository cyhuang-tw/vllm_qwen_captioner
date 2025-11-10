#!/usr/bin/env bash
set -euo pipefail

echo "=== vLLM Server Debugging ==="
echo ""

# Check GPU
echo "1. Checking GPU availability:"
nvidia-smi || echo "ERROR: nvidia-smi failed"
echo ""

# Check CUDA
echo "2. Checking CUDA:"
echo "CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-not set}"
echo ""

# Check container image
SIF_IMG="${SIF_IMG:-/work/hdd/bbjs/chuang14/apptainer/vllm_v2.sif}"
echo "3. Checking container image:"
ls -lh "$SIF_IMG" 2>&1 || echo "ERROR: Container image not found"
echo ""

# Check model path (if cached locally)
echo "4. Checking HuggingFace cache:"
echo "HF_HOME=${HF_HOME:-not set}"
ls -la ~/.cache/huggingface/ 2>&1 | head -20 || echo "No HF cache found"
echo ""

# Try a minimal vLLM test inside container
echo "5. Testing vLLM inside container:"
apptainer exec --cleanenv --nv \
  -B /u/chuang14 \
  -B /work/nvme/bbjs/chuang14 \
  -B /work/hdd/bbjs/chuang14 \
  -B /work/hdd/bbjs/shared \
  "$SIF_IMG" python3 -c "import vllm; print(f'vLLM version: {vllm.__version__}')" || echo "ERROR: vLLM import failed"
echo ""

echo "6. Testing CUDA inside container:"
apptainer exec --cleanenv --nv \
  -B /u/chuang14 \
  -B /work/nvme/bbjs/chuang14 \
  -B /work/hdd/bbjs/chuang14 \
  -B /work/hdd/bbjs/shared \
  "$SIF_IMG" python3 -c "import torch; print(f'PyTorch: {torch.__version__}'); print(f'CUDA available: {torch.cuda.is_available()}'); print(f'CUDA devices: {torch.cuda.device_count()}')" || echo "ERROR: CUDA check failed"
echo ""

echo "=== Debug complete ==="
echo ""
echo "Next steps:"
echo "1. If GPU not visible, check your SLURM allocation"
echo "2. If CUDA not available, there may be a driver issue"
echo "3. If import errors, the container may be corrupted"
echo "4. Check the FULL error log from start_server.sh"
