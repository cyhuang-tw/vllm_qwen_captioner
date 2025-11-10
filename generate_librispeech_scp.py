import os
import argparse
from pathlib import Path

def generate_scp_for_subset(subset_dir: Path, output_dir: Path, exts=(".flac", ".wav")):
    """Generate an .scp file for one LibriSpeech subset."""
    subset_name = subset_dir.name
    scp_path = output_dir / f"{subset_name}.scp"
    with open(scp_path, "w", encoding="utf-8") as f:
        for root, _, files in os.walk(subset_dir):
            for file in files:
                if file.endswith(exts):
                    abs_path = os.path.abspath(os.path.join(root, file))
                    utt_id = Path(file).stem  # e.g., 19-198-0000 -> utt_id
                    f.write(f"{utt_id} {abs_path}\n")
    print(f"[✓] Wrote {scp_path}")

def main():
    parser = argparse.ArgumentParser(description="Generate SCP files for each LibriSpeech subset.")
    parser.add_argument("--librispeech_dir", required=True, help="Root directory of LibriSpeech.")
    parser.add_argument("--output_dir", default=".", help="Directory to save generated SCP files.")
    args = parser.parse_args()

    librispeech_dir = Path(args.librispeech_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    subsets = [
        "train-clean-100",
        "train-clean-360",
        "train-other-500",
        "dev-clean",
        "dev-other",
        "test-clean",
        "test-other",
    ]

    for subset in subsets:
        subset_dir = librispeech_dir / subset
        if subset_dir.exists():
            generate_scp_for_subset(subset_dir, output_dir)
        else:
            print(f"[!] Skipped missing subset: {subset_dir}")

if __name__ == "__main__":
    main()

