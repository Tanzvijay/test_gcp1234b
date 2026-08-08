from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Form, Depends
from fastapi.security import APIKeyHeader
from typing import Optional

from auth import require_api_key                          # ← new
from tally_extractor import (
    run_brs, run_gst, run_provisions, run_ledger,
    run_tds, run_bills, run_stock,
    get_job, get_all_jobs,
    list_gcs_files, upload_from_request,
    BUCKET_NAME, list_gcs_folders, list_files_to_delete, delete_files_by_prefix,
)

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Tally XML Extractor API",
    description=(
        "Reads Tally XML exports from GCS. "
        "Heavy jobs run in background — poll /jobs/{job_id} for result.\n\n"
        "**Authentication:** every endpoint requires an `X-API-Key` header. "
        "Click the 🔒 **Authorize** button and enter your key."
    ),
    version="3.1.0",
    # Tell Swagger UI to offer the X-API-Key lock icon
    swagger_ui_parameters={"persistAuthorization": True},
)

# Register the security scheme so Swagger shows the Authorize button
app.openapi_tags = []          # populated below via tag groups

# ── Shorthand: inject auth on every route ─────────────────────────────────────
_auth = Depends(require_api_key)


# ── File management ───────────────────────────────────────────────────────────

@app.get("/files/to_delete", tags=["files"], dependencies=[_auth])
def list_files_to_delete_endpoint(
    prefix: str = Query(..., description="Folder prefix e.g. uploads_xml/"),
):
    """Preview files that would be deleted under a folder prefix — no actual deletion."""
    files = list_files_to_delete(BUCKET_NAME, prefix)
    if not files:
        raise HTTPException(status_code=404, detail=f"No files found under prefix {prefix!r}.")
    return {"files_to_delete": files, "count": len(files)}


@app.delete("/delete", tags=["files"], dependencies=[_auth])
def delete_files_endpoint(
    prefix: str = Query(..., description="Folder prefix to delete all files e.g. uploads_xml/"),
):
    """Delete all files under a folder prefix in the GCS bucket."""
    deleted = delete_files_by_prefix(BUCKET_NAME, prefix)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"No files found under prefix {prefix!r}.")
    return {"deleted": deleted, "count": len(deleted)}


@app.get("/List_folders", tags=["files"], dependencies=[_auth])
def list_folders():
    """Lists all top-level folders in the GCS bucket."""
    return list_gcs_folders(BUCKET_NAME)


@app.get("/files", tags=["files"], dependencies=[_auth])
def list_files(
    prefix: Optional[str] = Query(None, description="Folder prefix e.g. uploads_xml/"),
):
    """Lists all files in the GCS bucket, optionally filtered by prefix."""
    return list_gcs_files(BUCKET_NAME, prefix)


@app.post("/upload_gcs", tags=["files"], dependencies=[_auth])
async def upload_gcs_file(
    file: UploadFile = File(..., description="XML file to upload."),
):
    """
    Uploads a file to Google Cloud Storage under an auto-created MM-YYYY folder.
    Returns the GCS URI.
    """
    result = await upload_from_request(file, file.filename)
    return {"gcs_uri": result}


# ── Job status ────────────────────────────────────────────────────────────────

@app.get("/jobs/{job_id}", tags=["jobs"], dependencies=[_auth])
def job_status(job_id: str):
    """Poll this after starting any extraction. Returns status, result, error."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found.")
    return job


@app.get("/jobs", tags=["jobs"], dependencies=[_auth])
def list_jobs(limit: int = Query(50, ge=1, le=500)):
    """List the most recent jobs (newest first). Useful for debugging."""
    jobs = get_all_jobs()
    jobs.sort(key=lambda j: j["started_at"], reverse=True)
    return jobs[:limit]


# ── Extraction endpoints ───────────────────────────────────────────────────────

@app.get("/extract/brs", tags=["extraction"], dependencies=[_auth])
def brs_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start BRS extraction. Returns job_id immediately — poll /jobs/{job_id} for result."""
    job_id = run_brs(source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/gst", tags=["extraction"], dependencies=[_auth])
def gst_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start GST extraction. Returns job_id immediately — poll /jobs/{job_id} for result."""
    job_id = run_gst(source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/provisions", tags=["extraction"], dependencies=[_auth])
def provisions_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start month-end provisions extraction. Returns job_id immediately."""
    job_id = run_provisions(source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/ledger", tags=["extraction"], dependencies=[_auth])
def ledger_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start ledger transactions extraction. Returns job_id immediately."""
    job_id = run_ledger(source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/tds", tags=["extraction"], dependencies=[_auth])
def tds_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Base name for the PostgreSQL tables."),
):
    """Start TDS extraction. Result has tds_deducted and tds_paid (top 10 each)."""
    job_id = run_tds(source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/bills", tags=["extraction"], dependencies=[_auth])
def bills_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start bills extraction. Result has bill_types and outstanding (top 10 each)."""
    job_id = run_bills(source, file_name)
    return {"job_id": job_id, "status": "running"}


@app.get("/extract/stock", tags=["extraction"], dependencies=[_auth])
def stock_endpoint(
    source:    str = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: str = Query(..., description="Table name suffix saved to PostgreSQL."),
):
    """Start stock extraction. Returns job_id immediately."""
    job_id = run_stock(source, file_name)
    return {"job_id": job_id, "status": "running"}
