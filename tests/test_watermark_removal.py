#!/usr/bin/env python3
"""
Test script verifying VLM/LLM watermark removal and custom block_correction_prompt options.
"""
import json
import os
import sys
import time
import urllib.request

API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8090")
SAMPLE_PDF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample.pdf")

def create_sample_pdf(filepath: str):
    pdf_content = (
        b"%PDF-1.4\n"
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n"
        b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n"
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj\n"
        b"4 0 obj << /Length 190 >> stream\n"
        b"BT\n"
        b"/F1 18 Tf\n"
        b"50 720 Td\n"
        b"(Document Header) Tj\n"
        b"0 -40 Td\n"
        b"/F1 12 Tf\n"
        b"(This is a sample document containing confidential proprietary information.) Tj\n"
        b"0 -40 Td\n"
        b"/F1 10 Tf\n"
        b"(CONFIDENTIAL - INTERNAL USE ONLY - author@company.com) Tj\n"
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
        b"0000000486 00000 n \n"
        b"trailer << /Size 6 /Root 1 0 R >>\n"
        b"startxref\n"
        b"567\n"
        b"%%EOF\n"
    )
    with open(filepath, "wb") as f:
        f.write(pdf_content)

def encode_multipart_formdata(fields, files):
    boundary = "----WebKitFormBoundary" + hex(int(time.time() * 1000))[2:]
    body = bytearray()
    for key, value in fields.items():
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
        body.extend(f"{value}\r\n".encode())
    for key, (filename, content, content_type) in files.items():
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(f'Content-Disposition: form-data; name="{key}"; filename="{filename}"\r\n'.encode())
        body.extend(f"Content-Type: {content_type}\r\n\r\n".encode())
        body.extend(content)
        body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode())
    content_type = f"multipart/form-data; boundary={boundary}"
    return body, content_type

def test_openapi_schema():
    print("\n[Test 1] Checking OpenAPI Schema for watermark & prompt parameters...")
    req = urllib.request.Request(f"{API_BASE}/openapi.json")
    with urllib.request.urlopen(req, timeout=10) as resp:
        schema = json.loads(resp.read().decode())
    
    # Check CommonParams schema
    props = schema["components"]["schemas"]["CommonParams"]["properties"]
    assert "remove_watermarks" in props, "remove_watermarks not in CommonParams"
    assert "block_correction_prompt" in props, "block_correction_prompt not in CommonParams"
    assert "reasoning_effort" in props, "reasoning_effort not in CommonParams"
    assert "enable_thinking" in props, "enable_thinking not in CommonParams"
    print("  -> CommonParams contains remove_watermarks, block_correction_prompt, reasoning_effort, enable_thinking")

    # Check upload endpoints
    upload_body = schema["components"]["schemas"]["Body_convert_uploaded_file_marker_upload_post"]["properties"]
    assert "remove_watermarks" in upload_body, "remove_watermarks not in upload body"
    assert "block_correction_prompt" in upload_body, "block_correction_prompt not in upload body"
    assert "reasoning_effort" in upload_body, "reasoning_effort not in upload body"
    assert "enable_thinking" in upload_body, "enable_thinking not in upload body"

    async_body = schema["components"]["schemas"]["Body_convert_uploaded_file_async_marker_upload_async_post"]["properties"]
    assert "remove_watermarks" in async_body, "remove_watermarks not in async upload body"
    assert "block_correction_prompt" in async_body, "block_correction_prompt not in async upload body"
    assert "reasoning_effort" in async_body, "reasoning_effort not in async upload body"
    assert "enable_thinking" in async_body, "enable_thinking not in async upload body"
    print("  -> Upload & Async Upload schemas contain parameters")
    print("-> OpenAPI Schema Verification PASSED")

def test_async_watermark_submission():
    print("\n[Test 2] Submitting Async Job with remove_watermarks=True...")
    with open(SAMPLE_PDF, "rb") as f:
        pdf_bytes = f.read()

    fields = {
        "use_llm": "true",
        "mode": "fast",
        "remove_watermarks": "true",
        "output_format": "markdown",
    }
    files = {"file": ("watermark_test.pdf", pdf_bytes, "application/pdf")}
    data, content_type = encode_multipart_formdata(fields, files)

    req = urllib.request.Request(
        f"{API_BASE}/marker/upload/async",
        data=data,
        headers={"Content-Type": content_type},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        res = json.loads(resp.read().decode())
        job_id = res["job_id"]
        print(f"  -> Job Created successfully: {job_id}")
        assert res["status"] in ["queued", "processing"]
        assert res["poll_url"] == f"/marker/jobs/{job_id}"

    # Poll status for initial pipeline progression
    time.sleep(1.0)
    with urllib.request.urlopen(f"{API_BASE}/marker/jobs/{job_id}", timeout=10) as resp:
        info = json.loads(resp.read().decode())
        print(f"  -> Job stage: {info.get('stage')}, progress: {info.get('progress')}%, status: {info.get('status')}")
        assert info["status"] in ["queued", "processing", "completed"]
    print("-> Watermark removal submission PASSED")

def test_custom_prompt_submission():
    print("\n[Test 3] Submitting Async Job with custom block_correction_prompt...")
    with open(SAMPLE_PDF, "rb") as f:
        pdf_bytes = f.read()

    custom_prompt = (
        "You are a professional document cleanup specialist. Remove all background watermarks, "
        "confidentiality notices, and email addresses. Preserve genuine content."
    )
    fields = {
        "use_llm": "true",
        "mode": "fast",
        "block_correction_prompt": custom_prompt,
        "output_format": "markdown",
    }
    files = {"file": ("custom_prompt_test.pdf", pdf_bytes, "application/pdf")}
    data, content_type = encode_multipart_formdata(fields, files)

    req = urllib.request.Request(
        f"{API_BASE}/marker/upload/async",
        data=data,
        headers={"Content-Type": content_type},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        res = json.loads(resp.read().decode())
        job_id = res["job_id"]
        print(f"  -> Custom Prompt Job Created: {job_id}")
        assert res["status"] in ["queued", "processing"]

    time.sleep(1.0)
    with urllib.request.urlopen(f"{API_BASE}/marker/jobs/{job_id}", timeout=10) as resp:
        info = json.loads(resp.read().decode())
        print(f"  -> Custom Prompt Job stage: {info.get('stage')}, progress: {info.get('progress')}%")
        assert info["status"] in ["queued", "processing", "completed"]
    print("-> Custom prompt submission PASSED")

if __name__ == "__main__":
    create_sample_pdf(SAMPLE_PDF)
    test_openapi_schema()
    test_async_watermark_submission()
    test_custom_prompt_submission()
    print("\n==========================================")
    print("ALL WATERMARK & PROMPT TESTS PASSED!")
    print("==========================================")
