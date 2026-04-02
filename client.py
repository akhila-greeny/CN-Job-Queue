# client.py
# Job submission client — sends jobs to the server over a TLS-secured TCP connection.
#
# Protocol:
#   client → server : JSON {"job_id": int, "task": str}
#   server → client : "JOB_RECEIVED"  |  "ERROR:<reason>"

import socket
import ssl
import json
import time
import logging

# ── Configuration ────────────────────────────────────────────────────────────
HOST        = '127.0.0.1'
PORT        = 8080
CERTFILE    = 'cert.pem'   # server's certificate (for verification)
BUFFER_SIZE = 4096

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [CLIENT] %(levelname)s: %(message)s'
)
log = logging.getLogger(__name__)

# ── SSL Context ───────────────────────────────────────────────────────────────
def create_ssl_context() -> ssl.SSLContext:
    """Create a client-side TLS context that verifies the server certificate."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # Load the server's self-signed cert as a trusted CA
    ctx.load_verify_locations(cafile=CERTFILE)
    ctx.check_hostname = False   # using IP address, not a hostname
    ctx.verify_mode    = ssl.CERT_REQUIRED
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx

# ── Job Sender ────────────────────────────────────────────────────────────────
def send_job(ssl_ctx: ssl.SSLContext, job_id: int, task: str) -> bool:
    """
    Connect to the server, send a job, and wait for acknowledgement.
    Returns True if the server acknowledged the job, False otherwise.
    """
    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw_sock.settimeout(10.0)

    try:
        raw_sock.connect((HOST, PORT))
        conn = ssl_ctx.wrap_socket(raw_sock, server_hostname=HOST)

        job = {"job_id": job_id, "task": task}
        conn.sendall(json.dumps(job).encode())

        response = conn.recv(BUFFER_SIZE).decode().strip()

        if response == "JOB_RECEIVED":
            log.info("Job %d accepted by server ✓", job_id)
            return True
        else:
            log.warning("Unexpected server response for job %d: %s", job_id, response)
            return False

    except ssl.SSLError as e:
        log.error("TLS error sending job %d: %s", job_id, e)
        return False
    except socket.timeout:
        log.error("Timeout sending job %d", job_id)
        return False
    except ConnectionRefusedError:
        log.error("Connection refused — is the server running?")
        return False
    except Exception as e:
        log.error("Error sending job %d: %s", job_id, e)
        return False
    finally:
        raw_sock.close()

# ── Performance Benchmark ─────────────────────────────────────────────────────
def benchmark(ssl_ctx: ssl.SSLContext, num_jobs: int = 10):
    """Send `num_jobs` jobs and measure total elapsed time."""
    log.info("Starting benchmark: %d jobs", num_jobs)
    start = time.time()
    succeeded = 0

    for i in range(1, num_jobs + 1):
        ok = send_job(ssl_ctx, i, f"task_{i}")
        if ok:
            succeeded += 1
        time.sleep(0.1)   # slight spacing to avoid overwhelming the server

    elapsed = time.time() - start
    log.info(
        "Benchmark done: %d/%d jobs accepted in %.2fs (%.2f jobs/sec)",
        succeeded, num_jobs, elapsed, succeeded / elapsed
    )

# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ctx = create_ssl_context()

    # Normal run: send 5 jobs with 1-second gaps (matches original behaviour)
    for i in range(1, 6):
        send_job(ctx, i, f"task_{i}")
        time.sleep(1)

    # Uncomment to run a throughput benchmark instead:
    # benchmark(ctx, num_jobs=20)
