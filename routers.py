from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Form
from typing import Optional

from tally_extractor import (
    run_brs, run_gst, run_provisions, run_ledger,
    run_tds, run_bills, run_stock,
    get_job, get_all_jobs,
    list_gcs_files, upload_from_request,
    BUCKET_NAME, list_gcs_folders    
)

app = FastAPI(
    title="Tally XML Extractor API",
    description="Reads Tally XML exports from GCS. Heavy jobs run in background — poll /jobs/{job_id} for result.",
    version="3.0.0",
)


@app.delete("/delete", tags=["files"])
def delete_files(
    prefix: Optional[str] = Query(None, description="Folder prefix to delete all files e.g. uploads_xml/"),
    blob_name: Optional[str] = Query(None, description="Exact blob name to delete a single file e.g. uploads_xml/file.xml"),
):
    """Delete a single file or all files under a folder prefix in the GCS bucket."""
    from google.cloud import storage as gcs_storage

    if not prefix and not blob_name:
        raise HTTPException(status_code=400, detail="Provide either 'prefix' or 'blob_name'.")

    client = gcs_storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    deleted = []

    if blob_name:
        blob = bucket.blob(blob_name)
        if not blob.exists():
            raise HTTPException(status_code=404, detail=f"{blob_name!r} not found.")
        blob.delete()
        deleted.append(blob_name)
    else:
        blobs = list(bucket.list_blobs(prefix=prefix))
        if not blobs:
            raise HTTPException(status_code=404, detail=f"No files found under prefix {prefix!r}.")
        for blob in blobs:
            blob.delete()
            deleted.append(blob.name)

    return {"deleted": deleted, "count": len(deleted)}

# ── Job status ────────────────────────────────────────────────────────────────

@app.get("/jobs/{job_id}", tags=["jobs"])
def job_status(job_id: str):
    """Poll this after starting any extraction. Returns status, result, error."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found.")
    return job


@app.get("/jobs", tags=["jobs"])
def list_jobs(limit: int = Query(50, ge=1, le=500)):
    """List the most recent jobs (newest first). Useful for debugging."""
    jobs = get_all_jobs()
    jobs.sort(key=lambda j: j["started_at"], reverse=True)
    return jobs[:limit]



@app.get("/List_folders", tags=["files"])
def list_folders():
    """Lists all top-level folders in the GCS bucket."""
    return list_gcs_folders(BUCKET_NAME)


@app.get("/files", tags=["files"])
def list_files(
    prefix: Optional[str] = Query(None, description="Folder prefix e.g. uploads_xml/")
):
    """Lists all files in the GCS bucket, optionally filtered by prefix."""
    return list_gcs_files(BUCKET_NAME, prefix)


@app.post("/upload_gcs", tags=["files"])
async def upload_gcs_file(
    file: UploadFile = File(..., description="File to upload."),
    destination_blob_name: str = Form(..., description="Destination folder/name in GCS."),
):
    """Uploads a file to Google Cloud Storage. Returns the GCS URI."""
    gcs_uri = await upload_from_request(file, destination_blob_name)
    return {"gcs_uri": gcs_uri}


# ── Extraction endpoints ───────────────────────────────────────────────────────

@app.get("/extract/brs", tags=["extraction"])
def brs_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start BRS extraction. Returns job_id immediately — poll /jobs/{job_id} for result."""
    job_id = run_brs(source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/gst", tags=["extraction"])
def gst_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start GST extraction. Returns job_id immediately — poll /jobs/{job_id} for result."""
    job_id = run_gst(source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/provisions", tags=["extraction"])
def provisions_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start month-end provisions extraction. Returns job_id immediately."""
    job_id = run_provisions(source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/ledger", tags=["extraction"])
def ledger_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start ledger transactions extraction. Returns job_id immediately."""
    job_id = run_ledger(source, file_name)
    return {"job_id": job_id, "status": "running"}

@app.get("/extract/tds", tags=["extraction"])
def tds_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Base name for the PostgreSQL tables."),
):
    """Start TDS extraction. Result has tds_deducted and tds_paid (top 10 each)."""
    job_id = run_tds(source, file_name)
    return {"job_id": job_id, "status": "running"}

@app.get("/extract/bills", tags=["extraction"])
def bills_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start bills extraction. Result has bill_types and outstanding (top 10 each)."""
    job_id = run_bills(source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/stock", tags=["extraction"])
def stock_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start stock extraction. Returns job_id immediately."""
    job_id = run_stock(source, file_name)
    return {"job_id": job_id, "status": "running"}
