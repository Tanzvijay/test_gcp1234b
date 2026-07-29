"""
Tally XML Extractor API  –  v3.0
=================================
Problem solved
--------------
Large Tally XML files (1-2 GB) take several minutes to stream and parse.
Synchronous FastAPI handlers held the HTTP connection open until the OS or
a reverse-proxy (Cloud Run / nginx / ALB) killed it with a 504/timeout,
even though the DB write finished correctly.

Solution
--------
Every heavy extraction endpoint now returns immediately with a `job_id`.
The real work runs in a ThreadPoolExecutor background thread.
Callers poll  GET /jobs/{job_id}  until status == "done" | "error".

Architecture
------------
  POST /extract/brs?source=...&file_name=...
       → { "job_id": "uuid4" }               (returns in < 50 ms)

  GET  /jobs/{job_id}
       → { "job_id": ..., "status": "running" | "done" | "error",
           "result": <rows or null>, "error": <msg or null>,
           "started_at": ..., "finished_at": ... }

Job state is stored in an in-process dict (fine for single-instance
deployments; swap to Redis/DB for multi-replica setups).

All endpoints that previously were GET remain GET for back-compat.
"""

import uuid
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional, Any

from fastapi import FastAPI, Query, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse

from tally_extractor import (
    extract_bills,
    extract_brs,
    extract_gst,
    extract_month_end_provisions,
    extract_ledger_transactions,
    extract_tds,
    list_gcs_files,
    upload_from_request,
    BUCKET_NAME,
    list_gcs_folders,
    extract_stock,
    upload_file_to_gcs,
)

print("1. After importing tally_extractor")

# ── Job registry ──────────────────────────────────────────────────────────────
# { job_id: { status, result, error, started_at, finished_at } }
_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()

# One shared thread-pool – keeps background threads bounded.
# Increase max_workers if you expect many simultaneous large-file jobs.
_executor = ThreadPoolExecutor(max_workers=4)


def _new_job() -> str:
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id":      job_id,
            "status":      "running",
            "result":      None,
            "error":       None,
            "started_at":  datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
        }
    return job_id


def _run_job(job_id: str, fn, *args, **kwargs):
    """Execute fn(*args, **kwargs) in the thread-pool and update job state."""
    def _worker():
        try:
            result = fn(*args, **kwargs)
            with _jobs_lock:
                _jobs[job_id]["status"]      = "done"
                _jobs[job_id]["result"]      = result
                _jobs[job_id]["finished_at"] = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            with _jobs_lock:
                _jobs[job_id]["status"]      = "error"
                _jobs[job_id]["error"]       = str(exc)
                _jobs[job_id]["finished_at"] = datetime.now(timezone.utc).isoformat()

    _executor.submit(_worker)


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Tally XML Extractor API",
    description=(
        "Reads Tally XML exports from GCS and extracts BRS, GST, provisions, "
        "ledger transactions, TDS, Bills, and Stock.  "
        "Heavy jobs run in the background; poll /jobs/{job_id} for completion."
    ),
    version="3.0.0",
)


# ── Job status ────────────────────────────────────────────────────────────────
@app.get("/jobs/{job_id}", tags=["jobs"])
def get_job(job_id: str):
    """
    Poll this endpoint after submitting any extraction job.

    Returns
    -------
    status : "running" | "done" | "error"
    result : extracted rows (list/dict) once done, else null
    error  : error message if status == "error", else null
    """
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found.")
    return job


@app.get("/jobs", tags=["jobs"])
def list_jobs(limit: int = Query(50, ge=1, le=500)):
    """List the most recent `limit` jobs (newest first)."""
    with _jobs_lock:
        all_jobs = list(_jobs.values())
    all_jobs.sort(key=lambda j: j["started_at"], reverse=True)
    return all_jobs[:limit]


# ── Utility endpoints (unchanged, fast – no background needed) ────────────────
@app.get("/List_folders", tags=["files"])
def list_folders():
    """Lists all top-level folders in the GCS bucket."""
    return list_gcs_folders(BUCKET_NAME)


@app.post("/upload_gcs", tags=["files"])
async def upload_gcs_file(
    file: UploadFile = File(..., description="File to upload."),
    destination_blob_name: str = Form(..., description="Destination blob name in GCS."),
):
    """Uploads a file to Google Cloud Storage. Returns the GCS URI."""
    gcs_uri = await upload_from_request(file, destination_blob_name)
    return {"gcs_uri": gcs_uri}


@app.get("/files", tags=["files"])
def list_files(
    prefix: Optional[str] = Query(None, description="Folder prefix e.g. uploads_xml/")
):
    """Lists all files in the GCS bucket, optionally filtered by prefix."""
    return list_gcs_files(BUCKET_NAME, prefix)


# ── Extraction endpoints – all return a job_id immediately ────────────────────

@app.get("/extract/brs", tags=["extraction"])
def brs_endpoint(
    source:    str = Query(..., description="GCS URI  e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table-name suffix written to PostgreSQL."),
):
    """
    Start BRS extraction in the background.
    Poll  GET /jobs/{job_id}  for the result.
    """
    job_id = _new_job()
    _run_job(job_id, extract_brs, source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/gst", tags=["extraction"])
def gst_endpoint(
    source:    str = Query(..., description="GCS URI  e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table-name suffix written to PostgreSQL."),
):
    """Start GST extraction in the background."""
    job_id = _new_job()
    _run_job(job_id, extract_gst, source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/provisions", tags=["extraction"])
def provisions_endpoint(
    source:    str = Query(..., description="GCS URI  e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table-name suffix written to PostgreSQL."),
):
    """Start month-end provisions extraction in the background."""
    job_id = _new_job()
    _run_job(job_id, extract_month_end_provisions, source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/ledger", tags=["extraction"])
def ledger_endpoint(
    source:    str = Query(..., description="GCS URI  e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table-name suffix written to PostgreSQL."),
):
    """Start ledger transactions extraction in the background."""
    job_id = _new_job()
    _run_job(job_id, extract_ledger_transactions, source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/tds", tags=["extraction"])
def tds_endpoint(
    source:         str = Query(..., description="GCS URI  e.g. gs://bucket/file.xml"),
    deducted_table: str = Query(..., description="PostgreSQL table for TDS Deducted rows."),
    paid_table:     str = Query(..., description="PostgreSQL table for TDS Paid rows."),
):
    """
    Start TDS extraction in the background.
    Result contains two keys: tds_deducted and tds_paid (top 10 rows each).
    """
    job_id = _new_job()
    _run_job(job_id, extract_tds, source, deducted_table, paid_table)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/bills", tags=["extraction"])
def bills_endpoint(
    source:    str = Query(..., description="GCS URI  e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table-name suffix written to PostgreSQL."),
):
    """
    Start bills extraction in the background.
    Result contains bill_types (per bill-type top-10) and outstanding (top-10).
    """
    job_id = _new_job()
    _run_job(job_id, extract_bills, source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/stock", tags=["extraction"])
def stock_endpoint(
    source:    str = Query(..., description="GCS URI  e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table-name suffix written to PostgreSQL."),
):
    """Start stock extraction in the background."""
    job_id = _new_job()
    _run_job(job_id, extract_stock, source, file_name)
    return {"job_id": job_id, "status": "running"}
