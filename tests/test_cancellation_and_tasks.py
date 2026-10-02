#!/usr/bin/env python3
"""
測試腳本: 驗證任務監控清單與幽靈任務中斷功能 (Task Monitor & Job Cancellation)
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8090")
SAMPLE_PDF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample.pdf")

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

def test_list_jobs_endpoint():
    print("\n[Test 1] Testing GET /marker/jobs endpoint...")
    req = urllib.request.Request(f"{API_BASE}/marker/jobs")
    with urllib.request.urlopen(req, timeout=10) as resp:
        assert resp.status == 200
        jobs = json.loads(resp.read().decode())
        assert isinstance(jobs, list), "Response must be a list"
        print(f"  -> Successfully retrieved {len(jobs)} jobs from /marker/jobs")
        if len(jobs) > 0:
            sample = jobs[0]
            assert "job_id" in sample
            assert "status" in sample
            assert "progress" in sample
            assert "stage" in sample
            print(f"  -> Job fields verified on sample: {sample['job_id']}")
    print("-> GET /marker/jobs PASSED")

def test_cancel_job():
    print("\n[Test 2] Testing POST /marker/jobs/{job_id}/cancel...")
    with open(SAMPLE_PDF, "rb") as f:
        pdf_bytes = f.read()

    fields = {"use_llm": "false", "mode": "fast", "output_format": "markdown"}
    files = {"file": ("ghost_task_test.pdf", pdf_bytes, "application/pdf")}
    data, content_type = encode_multipart_formdata(fields, files)

    # 1. Create async job
    req = urllib.request.Request(f"{API_BASE}/marker/upload/async", data=data, headers={"Content-Type": content_type})
    with urllib.request.urlopen(req, timeout=20) as resp:
        res = json.loads(resp.read().decode())
        job_id = res["job_id"]
        print(f"  -> Created test job: {job_id}")

    # 2. Cancel the job immediately
    req_cancel = urllib.request.Request(
        f"{API_BASE}/marker/jobs/{job_id}/cancel",
        data=b"",
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req_cancel, timeout=10) as resp:
        assert resp.status == 200
        cancel_data = json.loads(resp.read().decode())
        assert cancel_data["success"] is True
        assert cancel_data["status"] == "cancelled"
        print(f"  -> Cancel response: {cancel_data['message']}")

    # 3. Verify status in /marker/jobs/{job_id}
    time.sleep(0.5)
    with urllib.request.urlopen(f"{API_BASE}/marker/jobs/{job_id}", timeout=10) as resp:
        job_info = json.loads(resp.read().decode())
        assert job_info["status"] == "cancelled"
        assert job_info["stage"] == "cancelled"
        assert "取消" in job_info["message"] or "手動" in job_info["message"]
        print(f"  -> Job status confirmed: {job_info['status']} ({job_info['message']})")
    print("-> POST /marker/jobs/{job_id}/cancel PASSED")

def test_cancel_nonexistent_job():
    print("\n[Test 3] Testing cancellation of non-existent job...")
    fake_id = "job_non_existent_999999"
    req_cancel = urllib.request.Request(
        f"{API_BASE}/marker/jobs/{fake_id}/cancel",
        data=b"",
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        urllib.request.urlopen(req_cancel, timeout=10)
        assert False, "Expected 404 error"
    except urllib.error.HTTPError as e:
        assert e.code == 404
        print("  -> Non-existent job returned HTTP 404 as expected")
    print("-> 404 Handling PASSED")

def test_cancel_running_job_and_queue_unblocks():
    print("\n[Test 4] Testing cancellation of running job and unblocking queued job...")
    with open(SAMPLE_PDF, "rb") as f:
        pdf_bytes = f.read()

    # Job 1: Balanced mode with LLM to simulate real workload
    fields1 = {"use_llm": "true", "mode": "balanced", "output_format": "markdown"}
    files1 = {"file": ("job1_running.pdf", pdf_bytes, "application/pdf")}
    data1, ct1 = encode_multipart_formdata(fields1, files1)

    req1 = urllib.request.Request(f"{API_BASE}/marker/upload/async", data=data1, headers={"Content-Type": ct1})
    with urllib.request.urlopen(req1, timeout=20) as resp:
        job1_id = json.loads(resp.read().decode())["job_id"]
        print(f"  -> Created Job 1: {job1_id}")

    # Wait until Job 1 starts processing
    for _ in range(30):
        time.sleep(0.5)
        with urllib.request.urlopen(f"{API_BASE}/marker/jobs/{job1_id}", timeout=10) as resp:
            j1 = json.loads(resp.read().decode())
            if j1["status"] == "processing":
                print(f"  -> Job 1 is now processing (stage={j1['stage']}, progress={j1['progress']}%)")
                break

    # Job 2: Fast mode, submitted while Job 1 is running
    fields2 = {"use_llm": "false", "mode": "fast", "output_format": "markdown"}
    files2 = {"file": ("job2_queued.pdf", pdf_bytes, "application/pdf")}
    data2, ct2 = encode_multipart_formdata(fields2, files2)

    req2 = urllib.request.Request(f"{API_BASE}/marker/upload/async", data=data2, headers={"Content-Type": ct2})
    with urllib.request.urlopen(req2, timeout=20) as resp:
        job2_id = json.loads(resp.read().decode())["job_id"]
        print(f"  -> Created Job 2: {job2_id}")

    # Verify Job 2 is queued or processing
    time.sleep(0.5)
    with urllib.request.urlopen(f"{API_BASE}/marker/jobs/{job2_id}", timeout=10) as resp:
        j2 = json.loads(resp.read().decode())
        print(f"  -> Job 2 initial status: {j2['status']} (queue_pos={j2.get('queue_position')})")

    # Cancel Job 1 mid-processing!
    print(f"  -> Cancelling Job 1 ({job1_id}) mid-flight...")
    req_cancel = urllib.request.Request(
        f"{API_BASE}/marker/jobs/{job1_id}/cancel",
        data=b"",
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req_cancel, timeout=10) as resp:
        c_res = json.loads(resp.read().decode())
        assert c_res["success"] is True
        print(f"  -> Cancelled Job 1 successfully: {c_res['message']}")

    # Verify Job 1 is cancelled
    with urllib.request.urlopen(f"{API_BASE}/marker/jobs/{job1_id}", timeout=10) as resp:
        j1_after = json.loads(resp.read().decode())
        assert j1_after["status"] == "cancelled"
        print(f"  -> Job 1 verified cancelled: status={j1_after['status']}")

    # Now verify Job 2 immediately acquires semaphore and becomes processing or completed!
    unblocked = False
    for i in range(40):
        time.sleep(0.5)
        with urllib.request.urlopen(f"{API_BASE}/marker/jobs/{job2_id}", timeout=10) as resp:
            j2_check = json.loads(resp.read().decode())
            status = j2_check["status"]
            if status in ("processing", "completed"):
                print(f"  -> SUCCESS! Job 2 unblocked and transitioned to: {status} (progress={j2_check['progress']}%)")
                unblocked = True
                break
            else:
                print(f"     waiting for Job 2 to unblock... current status: {status}")

    assert unblocked, f"Job 2 was never unblocked! It remained in queued state."
    print("-> Cancellation and Queue Unblock PASSED")

if __name__ == "__main__":
    test_list_jobs_endpoint()
    test_cancel_job()
    test_cancel_nonexistent_job()
    test_cancel_running_job_and_queue_unblocks()
    print("\n==========================================")
    print("ALL TASK MONITOR & CANCELLATION TESTS PASSED!")
    print("==========================================")

