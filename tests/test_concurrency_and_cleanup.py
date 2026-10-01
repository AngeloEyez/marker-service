#!/usr/bin/env python3
"""
Test script for Marker Service Concurrency Limiting (FIFO Queue) & Cleanup Mechanism
"""
import sys
import os
import time
import requests
import concurrent.futures

BASE_URL = "http://localhost:8090"
SAMPLE_PDF = os.path.join(os.path.dirname(__file__), "sample.pdf")

def submit_job(idx):
    url = f"{BASE_URL}/marker/upload/async"
    with open(SAMPLE_PDF, "rb") as f:
        files = {"file": (f"concurrency_test_{idx}.pdf", f, "application/pdf")}
        data = {
            "use_llm": "false",
            "mode": "fast",
            "paginate_output": "true"
        }
        res = requests.post(url, files=files, data=data)
        res.raise_for_status()
        return idx, res.json()

def test_queue_and_cleanup():
    print(f"[{time.strftime('%X')}] 1. Checking initial /health status...")
    h = requests.get(f"{BASE_URL}/health").json()
    print(f"   Max concurrent: {h['queue_status']['max_concurrent_limit']}")
    print(f"   Active jobs: {h['queue_status']['active_processing_jobs']}")
    print(f"   Queued jobs: {h['queue_status']['queued_waiting_jobs']}")

    print(f"\n[{time.strftime('%X')}] 2. Submitting 3 conversion requests simultaneously...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(submit_job, i) for i in range(3)]
        results = [f.result() for f in futures]

    jobs = []
    for idx, r in results:
        job_id = r["job_id"]
        status = r["status"]
        q_pos = r.get("queue_position")
        print(f"   Job {idx}: {job_id} -> initial status: {status}, queue_position: {q_pos}")
        jobs.append((idx, job_id))

    print(f"\n[{time.strftime('%X')}] 3. Monitoring queue transitions...")
    completed = set()
    start_time = time.time()
    
    while len(completed) < len(jobs):
        for idx, job_id in jobs:
            if job_id in completed:
                continue
            res = requests.get(f"{BASE_URL}/marker/jobs/{job_id}").json()
            st = res.get("status")
            pos = res.get("queue_position")
            prog = res.get("progress_percentage", 0)
            stage = res.get("stage", "")
            print(f"   [{time.strftime('%X')}] Job {idx} ({job_id[:16]}...): status={st}, q_pos={pos}, progress={prog}%, stage={stage}")
            
            if st == "completed":
                completed.add(job_id)
                md_len = len(res.get('result', {}).get('output', ''))
                print(f"   >>> Job {idx} COMPLETED! Markdown length: {md_len}")
            elif st == "failed":
                print(f"   >>> Job {idx} FAILED: {res.get('error')}")
                sys.exit(1)
        
        if len(completed) < len(jobs):
            time.sleep(2)
        if time.time() - start_time > 120:
            print("TIMEOUT waiting for jobs to complete.")
            sys.exit(1)

    print(f"\n[{time.strftime('%X')}] 4. Checking /health after all jobs finished...")
    h = requests.get(f"{BASE_URL}/health").json()
    print(f"   Active jobs: {h['queue_status']['active_processing_jobs']}")
    print(f"   Queued jobs: {h['queue_status']['queued_waiting_jobs']}")
    assert h['queue_status']['active_processing_jobs'] == 0
    assert h['queue_status']['queued_waiting_jobs'] == 0

    print(f"\n[{time.strftime('%X')}] 5. Verifying uploaded file cleanup...")
    # Check if any 'concurrency_test_' files remain in uploads
    uploads_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "uploads")
    remaining_concurrency_files = [f for f in os.listdir(uploads_dir) if "concurrency_test" in f]
    print(f"   Remaining concurrency_test files in uploads/: {remaining_concurrency_files}")
    assert len(remaining_concurrency_files) == 0, f"Files were not cleaned up: {remaining_concurrency_files}"

    print(f"\n[{time.strftime('%X')}] SUCCESS: All concurrency queue & cleanup tests passed!")

if __name__ == "__main__":
    test_queue_and_cleanup()
