# server.py
# Distributed Job Queue Server with SSL/TLS
# Handles multiple clients and workers concurrently over secure TCP sockets

import socket
import ssl
import threading
import json
import queue
import time
import logging

# ── Configuration ────────────────────────────────────────────────────────────
HOST        = '127.0.0.1'
PORT        = 8080
CERTFILE    = 'cert.pem'
KEYFILE     = 'key.pem'
BUFFER_SIZE = 4096

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [SERVER] %(levelname)s: %(message)s'
)
log = logging.getLogger(__name__)

# ── Shared State ──────────────────────────────────────────────────────────────
job_queue   = queue.Queue()   # pending jobs
in_progress = {}              # job_id -> job dict (jobs currently with a worker)
lock        = threading.Lock()

# ── Performance Counters ──────────────────────────────────────────────────────
stats = {
    "jobs_received":   0,
    "jobs_completed":  0,
    "jobs_requeued":   0,
    "workers_crashed": 0,
}
stats_lock = threading.Lock()

def increment_stat(key: str, amount: int = 1):
    with stats_lock:
        stats[key] += amount

# ── SSL Context ───────────────────────────────────────────────────────────────
def create_ssl_context() -> ssl.SSLContext:
    """Create a server-side TLS context."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=CERTFILE, keyfile=KEYFILE)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx

# ── Client Handler ────────────────────────────────────────────────────────────
def client_handler(conn: ssl.SSLSocket, addr, first_msg: str):
    """
    Receives a JSON job from a client, enqueues it, and ACKs.
    Protocol:
        client → server : JSON job object
        server → client : "JOB_RECEIVED"
    """
    try:
        job = json.loads(first_msg)

        if "job_id" not in job or "task" not in job:
            conn.sendall("ERROR:Invalid job format".encode())
            log.warning("Received malformed job from %s", addr)
            return

        with lock:
            job_queue.put(job)

        increment_stat("jobs_received")
        log.info("Job enqueued from %s: %s", addr, job)
        conn.sendall("JOB_RECEIVED".encode())

    except json.JSONDecodeError:
        log.error("JSON decode error from client %s", addr)
        try:
            conn.sendall("ERROR:Invalid JSON".encode())
        except Exception:
            pass
    except Exception as e:
        log.error("Unexpected error in client_handler for %s: %s", addr, e)
    finally:
        conn.close()

# ── Worker Handler ─────────────────────────────────────────────────────────────
def worker_handler(conn: ssl.SSLSocket, addr):
    """
    Manages a worker connection through its full lifecycle:
        server → worker : "READY"
        worker → server : "GET_JOB"
        server → worker : JSON job  OR  "NO_JOB"
        worker → server : "DONE" | "FAILED"
        server updates in_progress accordingly

    On any failure, the in-progress job is re-queued automatically.
    """
    job = None  # track which job was handed to this worker

    try:
        # Step 1: signal readiness
        conn.sendall("READY".encode())

        # Step 2: wait for worker request
        data = conn.recv(BUFFER_SIZE).decode().strip()
        if data != "GET_JOB":
            log.warning("Unexpected message from worker %s: %s", addr, data)
            return

        # Step 3: try to dequeue a job
        with lock:
            if job_queue.empty():
                conn.sendall("NO_JOB".encode())
                log.info("No jobs available for worker %s", addr)
                return

            job = job_queue.get()
            in_progress[job["job_id"]] = job

        log.info("Dispatched job %s to worker %s", job["job_id"], addr)
        conn.sendall(json.dumps(job).encode())

        # Step 4: wait for result (with timeout so crashed workers don't block)
        conn.settimeout(30.0)
        try:
            result = conn.recv(BUFFER_SIZE).decode().strip()
        except socket.timeout:
            raise RuntimeError("Worker timed out without responding")

        if result == "DONE":
            with lock:
                in_progress.pop(job["job_id"], None)
            increment_stat("jobs_completed")
            log.info("Job %s completed by worker %s", job["job_id"], addr)
            job = None  # mark as cleanly handled

        elif result == "FAILED":
            raise RuntimeError(f"Worker reported failure for job {job['job_id']}")

        else:
            raise RuntimeError(f"Unexpected result '{result}' from worker {addr}")

    except Exception as e:
        increment_stat("workers_crashed")
        log.error("Worker %s error: %s — re-queuing job if applicable", addr, e)

        # Re-queue the job that was in progress for THIS worker only
        if job is not None:
            with lock:
                in_progress.pop(job["job_id"], None)
                job_queue.put(job)
            increment_stat("jobs_requeued")
            log.info("Job %s re-queued after worker failure", job["job_id"])

    finally:
        conn.close()

# ── Stats Reporter ─────────────────────────────────────────────────────────────
def stats_reporter(interval: int = 10):
    """Background thread: prints server stats every `interval` seconds."""
    while True:
        time.sleep(interval)
        with stats_lock:
            log.info(
                "STATS | received=%d  completed=%d  requeued=%d  crashes=%d  queue_size=%d  in_progress=%d",
                stats["jobs_received"],
                stats["jobs_completed"],
                stats["jobs_requeued"],
                stats["workers_crashed"],
                job_queue.qsize(),
                len(in_progress),
            )

# ── Main Server Loop ───────────────────────────────────────────────────────────
def start_server():
    ssl_ctx = create_ssl_context()

    raw_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    raw_server.bind((HOST, PORT))
    raw_server.listen(50)

    log.info("Listening on %s:%d (TLS enabled)", HOST, PORT)

    # Start background stats reporter
    threading.Thread(target=stats_reporter, args=(10,), daemon=True).start()

    while True:
        try:
            raw_conn, addr = raw_server.accept()

            # Wrap accepted socket with TLS
            try:
                conn = ssl_ctx.wrap_socket(raw_conn, server_side=True)
            except ssl.SSLError as e:
                log.error("TLS handshake failed from %s: %s", addr, e)
                raw_conn.close()
                continue

            # Peek at first message to identify sender type
            try:
                first_msg = conn.recv(BUFFER_SIZE).decode().strip()
            except Exception as e:
                log.error("Failed to read first message from %s: %s", addr, e)
                conn.close()
                continue

            if first_msg == "WORKER":
                threading.Thread(
                    target=worker_handler, args=(conn, addr), daemon=True
                ).start()
            else:
                threading.Thread(
                    target=client_handler, args=(conn, addr, first_msg), daemon=True
                ).start()

        except KeyboardInterrupt:
            log.info("Server shutting down.")
            break
        except Exception as e:
            log.error("Accept loop error: %s", e)

    raw_server.close()

if __name__ == "__main__":
    start_server()
