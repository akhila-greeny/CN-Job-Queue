# worker.py
# Worker node — polls the server for jobs over a TLS-secured TCP connection,
# processes them, and reports completion (or failure).
#
# Protocol:
#   worker → server : "WORKER"
#   server → worker : "READY"
#   worker → server : "GET_JOB"
#   server → worker : JSON job  |  "NO_JOB"
#   worker → server : "DONE"    |  "FAILED"

import socket
import ssl
import json
import time
import random
import logging

# ── Configuration ────────────────────────────────────────────────────────────
HOST         = '127.0.0.1'
PORT         = 8080
CERTFILE     = 'cert.pem'
BUFFER_SIZE  = 4096
CRASH_RATE   = 0.2    # probability of simulated crash (20%)
POLL_INTERVAL = 2.0   # seconds to wait when no job is available

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [WORKER] %(levelname)s: %(message)s'
)
log = logging.getLogger(__name__)

# ── SSL Context ───────────────────────────────────────────────────────────────
def create_ssl_context() -> ssl.SSLContext:
    """Create a client-side TLS context that verifies the server certificate."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=CERTFILE)
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_REQUIRED
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx

# ── Job Processing ────────────────────────────────────────────────────────────
def process_job(job: dict) -> bool:
    """
    Simulate job processing.
    Returns True on success, False on simulated failure.
    """
    log.info("Processing job %s: %s", job["job_id"], job["task"])
    time.sleep(2)  # simulate work

    # Simulate an occasional processing error (distinct from a crash)
    if random.random() < 0.05:
        log.warning("Job %s processing failed (simulated error)", job["job_id"])
        return False

    log.info("Job %s processed successfully", job["job_id"])
    return True

# ── Single Worker Cycle ───────────────────────────────────────────────────────
def run_once(ssl_ctx: ssl.SSLContext) -> str:
    """
    One full request-process-report cycle.
    Returns one of: "done", "no_job", "crashed", "failed", "error"
    """
    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw_sock.settimeout(15.0)

    try:
        raw_sock.connect((HOST, PORT))
        conn = ssl_ctx.wrap_socket(raw_sock, server_hostname=HOST)

        # Identify as a worker
        conn.sendall("WORKER".encode())

        # Wait for READY signal
        ready = conn.recv(BUFFER_SIZE).decode().strip()
        if ready != "READY":
            log.warning("Expected READY, got: %s", ready)
            return "error"

        # Request a job
        conn.sendall("GET_JOB".encode())

        # Receive job or NO_JOB
        data = conn.recv(BUFFER_SIZE).decode().strip()

        if data == "NO_JOB":
            log.info("No jobs available, will retry in %.1fs", POLL_INTERVAL)
            return "no_job"

        # Parse the job
        try:
            job = json.loads(data)
        except json.JSONDecodeError:
            log.error("Could not parse job JSON: %s", data)
            conn.sendall("FAILED".encode())
            return "error"

        # Simulate a crash BEFORE processing (worker just drops the connection)
        if random.random() < CRASH_RATE:
            log.warning("SIMULATED CRASH — dropping connection for job %s", job["job_id"])
            # Deliberately do NOT send DONE/FAILED; server detects timeout/disconnect
            return "crashed"

        # Process the job
        success = process_job(job)

        if success:
            conn.sendall("DONE".encode())
            log.info("Reported DONE for job %s", job["job_id"])
            return "done"
        else:
            conn.sendall("FAILED".encode())
            log.info("Reported FAILED for job %s", job["job_id"])
            return "failed"

    except ssl.SSLError as e:
        log.error("TLS error: %s", e)
        return "error"
    except socket.timeout:
        log.error("Socket timed out")
        return "error"
    except ConnectionRefusedError:
        log.error("Connection refused — is the server running?")
        return "error"
    except Exception as e:
        log.error("Unexpected worker error: %s", e)
        return "error"
    finally:
        raw_sock.close()

# ── Worker Loop ───────────────────────────────────────────────────────────────
def worker_loop():
    ssl_ctx = create_ssl_context()
    log.info("Worker started, connecting to %s:%d", HOST, PORT)

    while True:
        outcome = run_once(ssl_ctx)

        if outcome == "no_job":
            time.sleep(POLL_INTERVAL)
        elif outcome == "crashed":
            # After a simulated crash, pause briefly before reconnecting
            time.sleep(1.0)
        elif outcome in ("done", "failed"):
            time.sleep(0.5)   # short cooldown between jobs
        else:
            # error / unknown — back off before retrying
            time.sleep(3.0)

if __name__ == "__main__":
    worker_loop()
