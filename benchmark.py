# benchmark.py
# Performance evaluation script — sends N concurrent jobs to the server
# and measures throughput, latency, and success rate.
#
# Run AFTER starting server.py and at least one worker.py instance.

import socket
import ssl
import json
import time
import threading
import logging
import statistics

HOST        = '127.0.0.1'
PORT        = 8080
CERTFILE    = 'cert.pem'
BUFFER_SIZE = 4096

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [BENCH] %(levelname)s: %(message)s'
)
log = logging.getLogger(__name__)

def create_ssl_context():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=CERTFILE)
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_REQUIRED
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx

results = []
results_lock = threading.Lock()

def send_job(ssl_ctx, job_id):
    """Send one job and record its round-trip latency."""
    start = time.time()
    success = False

    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw_sock.settimeout(10.0)

    try:
        raw_sock.connect((HOST, PORT))
        conn = ssl_ctx.wrap_socket(raw_sock, server_hostname=HOST)
        job = {"job_id": job_id, "task": f"bench_task_{job_id}"}
        conn.sendall(json.dumps(job).encode())
        response = conn.recv(BUFFER_SIZE).decode().strip()
        success = (response == "JOB_RECEIVED")
    except Exception as e:
        log.debug("Job %d failed: %s", job_id, e)
    finally:
        raw_sock.close()

    latency = (time.time() - start) * 1000  # ms

    with results_lock:
        results.append({"job_id": job_id, "latency_ms": latency, "success": success})

def run_benchmark(num_jobs=50, concurrency=10):
    ssl_ctx = create_ssl_context()
    log.info("Benchmark: %d jobs, concurrency=%d", num_jobs, concurrency)

    start = time.time()
    threads = []

    for i in range(1, num_jobs + 1):
        t = threading.Thread(target=send_job, args=(ssl_ctx, i))
        threads.append(t)
        t.start()

        # Throttle: only keep `concurrency` threads in flight at once
        if len([x for x in threads if x.is_alive()]) >= concurrency:
            time.sleep(0.05)

    for t in threads:
        t.join()

    elapsed = time.time() - start

    # ── Report ────────────────────────────────────────────────────────────────
    latencies = [r["latency_ms"] for r in results]
    successes = sum(1 for r in results if r["success"])

    log.info("=" * 55)
    log.info("BENCHMARK RESULTS")
    log.info("=" * 55)
    log.info("Total jobs sent   : %d", num_jobs)
    log.info("Successful ACKs   : %d (%.1f%%)", successes, 100 * successes / num_jobs)
    log.info("Total time        : %.2f s", elapsed)
    log.info("Throughput        : %.2f jobs/sec", num_jobs / elapsed)
    log.info("Latency (mean)    : %.2f ms", statistics.mean(latencies))
    log.info("Latency (median)  : %.2f ms", statistics.median(latencies))
    log.info("Latency (p95)     : %.2f ms", sorted(latencies)[int(0.95 * len(latencies))])
    log.info("Latency (max)     : %.2f ms", max(latencies))
    log.info("=" * 55)

if __name__ == "__main__":
    run_benchmark(num_jobs=50, concurrency=10)
