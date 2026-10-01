#!/usr/bin/env python3
"""
Unit test for Active Task Whitelist Protection & Daily Retention Policy
"""
import os
import sys
import time
import requests
import concurrent.futures

BASE_URL = "http://localhost:8090"
SAMPLE_PDF = os.path.join(os.path.dirname(__file__), "sample.pdf")

def submit_async(idx):
    with open(SAMPLE_PDF, "rb") as f:
        files = {"file": (f"whitelist_test_{idx}.pdf", f, "application/pdf")}
        data = {"use_llm": "false", "mode": "fast", "paginate_output": "true"}
        r = requests.post(f"{BASE_URL}/marker/upload/async", files=files, data=data)
        r.raise_for_status()
        return r.json()

def test_whitelist():
    print(f"[{time.strftime('%X')}] 1. Checking /health retention policy...")
    h = requests.get(f"{BASE_URL}/health").json()
    rp = h["retention_policy"]
    print(f"   Cleanup interval: {rp['cleanup_interval_seconds']}s (expected 86400s)")
    print(f"   File retention: {rp['file_retention_seconds']}s (expected 86400s)")
    print(f"   Job retention: {rp['job_retention_seconds']}s (expected 86400s)")
    assert rp["cleanup_interval_seconds"] == 86400
    assert rp["file_retention_seconds"] == 86400
    assert rp["job_retention_seconds"] == 86400

    print(f"\n[{time.strftime('%X')}] 2. Submitting 3 simultaneous requests to trigger queuing...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(submit_async, i) for i in range(3)]
        jobs = [f.result() for f in futures]

    for j in jobs:
        print(f"   Job {j['job_id']}: status={j['status']}, q_pos={j.get('queue_position')}")

    # Immediately check health to verify protected active tasks count
    h_active = requests.get(f"{BASE_URL}/health").json()
    protected_count = h_active["retention_policy"]["protected_active_tasks_count"]
    print(f"   Active protected tasks count during execution: {protected_count}")
    assert protected_count >= 1, "There should be at least 1 active protected task!"

    print(f"\n[{time.strftime('%X')}] 3. Waiting for all jobs to complete...")
    for j in jobs:
        jid = j["job_id"]
        while True:
            st = requests.get(f"{BASE_URL}/marker/jobs/{jid}").json().get("status")
            if st in ["completed", "failed"]:
                print(f"   Job {jid} finished: {st}")
                break
            time.sleep(1)

    print(f"\n[{time.strftime('%X')}] 4. Checking /health after completion...")
    h_end = requests.get(f"{BASE_URL}/health").json()
    assert h_end["retention_policy"]["protected_active_tasks_count"] == 0
    print("   Active protected tasks count: 0 (all released cleanly)")

    print(f"\n[{time.strftime('%X')}] SUCCESS: Whitelist and retention policy verification passed!")

if __name__ == "__main__":
    test_whitelist()
