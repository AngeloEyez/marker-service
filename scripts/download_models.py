#!/usr/bin/env python3
"""
Pre-download Surya GGUF models for llama-server.
"""
import os
import sys
from huggingface_hub import hf_hub_download

TARGET_DIR = os.getenv("MODELS_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models"))
REPO_ID = "datalab-to/surya-ocr-2-gguf"
FILES = ["surya-2.gguf", "surya-2-mmproj.gguf"]

def main():
    os.makedirs(TARGET_DIR, exist_ok=True)
    print(f"Target directory for models: {TARGET_DIR}")
    for filename in FILES:
        dest = os.path.join(TARGET_DIR, filename)
        if os.path.exists(dest):
            print(f"[OK] {filename} already exists ({os.path.getsize(dest)} bytes). Skipping download.")
            continue
        print(f"[Downloading] {filename} from {REPO_ID}...")
        downloaded = hf_hub_download(
            repo_id=REPO_ID,
            filename=filename,
            local_dir=TARGET_DIR,
            local_dir_use_symlinks=False
        )
        print(f"[Finished] {filename} saved to {downloaded}")

if __name__ == "__main__":
    main()
