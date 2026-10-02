#!/usr/bin/env python3
"""
Test script for Marker Microservice API.
Tests health endpoint, document conversion, and optional LLM enhancement.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.parse

API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8090")
SAMPLE_PDF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample.pdf")

def create_sample_pdf(filepath: str):
    """Generate a minimal valid 1-page PDF file with text and a table for testing."""
    pdf_content = (
        b"%PDF-1.4\n"
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n"
        b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n"
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj\n"
        b"4 0 obj << /Length 130 >> stream\n"
        b"BT\n"
        b"/F1 18 Tf\n"
        b"50 720 Td\n"
        b"(Marker Document Conversion Test Document) Tj\n"
        b"0 -40 Td\n"
        b"/F1 12 Tf\n"
        b"(This is an automated test page verifying GPU accelerated layout and OCR.) Tj\n"
        b"ET\n"
        b"endstream\n"
        b"endobj\n"
        b"5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n"
        b"xref\n"
        b"0 6\n"
        b"0000000000 65535 f \n"
        b"0000000009 00000 n \n"
        b"0000000058 00000 n \n"
        b"0000000115 00000 n \n"
        b"0000000244 00000 n \n"
        b"0000000426 00000 n \n"
        b"trailer << /Size 6 /Root 1 0 R >>\n"
        b"startxref\n"
        b"507\n"
        b"%%EOF\n"
    )
    with open(filepath, "wb") as f:
        f.write(pdf_content)
    print(f"[Generated] Sample PDF created at {filepath} ({len(pdf_content)} bytes)")

def test_health():
    url = f"{API_BASE}/health"
    print(f"\n[Test 1] Checking Health Endpoint: {url}...")
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            print(json.dumps(data, indent=2, ensure_ascii=False))
            assert data.get("status") in ["healthy", "ok", "degraded (no cuda)"], "Unexpected status"
            print("-> Health Check PASSED")
            return data
    except Exception as e:
        print(f"-> Health Check FAILED: {e}")
        return None

def test_upload(use_llm=False):
    url = f"{API_BASE}/marker/upload"
    mode_str = "with --use_llm (Qwen3.8-27B)" if use_llm else "standard mode"
    print(f"\n[Test 2] Testing PDF Conversion ({mode_str}): {url}...")
    
    if not os.path.exists(SAMPLE_PDF):
        create_sample_pdf(SAMPLE_PDF)

    boundary = f"----WebKitFormBoundary{int(time.time()*1000)}"
    body = bytearray()

    # Form field: use_llm
    body.extend(f"--{boundary}\r\n".encode())
    body.extend(b'Content-Disposition: form-data; name="use_llm"\r\n\r\n')
    body.extend(f"{str(use_llm).lower()}\r\n".encode())

    # Form field: output_format
    body.extend(f"--{boundary}\r\n".encode())
    body.extend(b'Content-Disposition: form-data; name="output_format"\r\n\r\n')
    body.extend(b"markdown\r\n")

    # File field
    with open(SAMPLE_PDF, "rb") as f:
        file_bytes = f.read()
    body.extend(f"--{boundary}\r\n".encode())
    body.extend(f'Content-Disposition: form-data; name="file"; filename="{os.path.basename(SAMPLE_PDF)}"\r\n'.encode())
    body.extend(b"Content-Type: application/pdf\r\n\r\n")
    body.extend(file_bytes)
    body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode())

    req = urllib.request.Request(
        url,
        data=bytes(body),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            result = json.loads(resp.read().decode())
            print(f"-> Status Code: {resp.status}")
            print(f"-> Success: {result.get('success')}")
            if result.get("success"):
                output_text = result.get("output", "")
                print(f"-> Output preview:\n{output_text[:300]}...")
                print("-> Conversion PASSED")
            else:
                print(f"-> Error from server: {result.get('error')}")
            return result
    except Exception as e:
        print(f"-> Conversion FAILED: {e}")
        return None

def test_async_and_zip():
    import zipfile
    url = f"{API_BASE}/marker/upload/async"
    print(f"\n[Test 3] Testing Async Upload and ZIP Download: {url}...")

    boundary = f"----WebKitFormBoundary{int(time.time()*1000)}"
    body = bytearray()
    body.extend(f"--{boundary}\r\n".encode())
    body.extend(b'Content-Disposition: form-data; name="use_llm"\r\n\r\nfalse\r\n')
    with open(SAMPLE_PDF, "rb") as f:
        file_bytes = f.read()
    body.extend(f"--{boundary}\r\n".encode())
    body.extend(f'Content-Disposition: form-data; name="file"; filename="{os.path.basename(SAMPLE_PDF)}"\r\n'.encode())
    body.extend(b"Content-Type: application/pdf\r\n\r\n")
    body.extend(file_bytes)
    body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode())

    req = urllib.request.Request(
        url,
        data=bytes(body),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST"
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        job_info = json.loads(resp.read().decode())
        job_id = job_info["job_id"]
        print(f"-> Async Job Created: {job_id}")

    # Poll until completed
    job_finished = False
    for _ in range(60):
        req_poll = urllib.request.Request(f"{API_BASE}/marker/jobs/{job_id}")
        with urllib.request.urlopen(req_poll, timeout=10) as r:
            status_data = json.loads(r.read().decode())
            status = status_data.get("status")
            if status in ["completed", "failed"]:
                print(f"-> Job finished with status: {status} (elapsed: {status_data.get('elapsed_seconds')}s)")
                assert status == "completed", f"Job failed: {status_data.get('error')}"
                job_finished = True
                break
        time.sleep(2)
    assert job_finished, f"Job {job_id} did not complete within timeout"

    # Test ZIP download
    download_url = f"{API_BASE}/marker/jobs/{job_id}/download"
    print(f"-> Downloading ZIP from: {download_url}...")
    req_dl = urllib.request.Request(download_url)
    with urllib.request.urlopen(req_dl, timeout=30) as r:
        assert r.status == 200, f"Expected 200, got {r.status}"
        zip_bytes = r.read()
        print(f"-> Downloaded ZIP size: {len(zip_bytes)} bytes")
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            namelist = zf.namelist()
            print(f"-> Files in ZIP: {namelist}")
            assert any(f.endswith(".md") for f in namelist), "Markdown file missing in ZIP"
            assert "metadata.json" in namelist, "metadata.json missing in ZIP"
    print("-> Async and ZIP Download Test PASSED")

if __name__ == "__main__":
    import io
    create_sample_pdf(SAMPLE_PDF)
    h = test_health()
    if h:
        test_upload(use_llm=False)
        test_async_and_zip()

