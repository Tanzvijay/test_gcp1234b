
# routers.py  — add these lines at the very top

from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Form
from typing import Optional

app = FastAPI(
    title="Tally XML Extractor API",
    version="3.0.0",
)




from tally_extractor import (
    run_brs, run_gst, run_provisions, run_ledger,
    run_tds, run_bills, run_stock,
    get_job, get_all_jobs,
    list_gcs_files, upload_from_request,
    BUCKET_NAME, list_gcs_folders,
)

@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found.")
    return job

@app.get("/jobs")
def list_jobs(limit: int = Query(50)):
    jobs = get_all_jobs()
    jobs.sort(key=lambda j: j["started_at"], reverse=True)
    return jobs[:limit]

@app.get("/extract/brs")
def brs_endpoint(source: str = Query(...), file_name: str = Query(...)):
    job_id = run_brs(source, file_name)
    return {"job_id": job_id, "status": "running"}

@app.get("/extract/gst")
def gst_endpoint(source: str = Query(...), file_name: str = Query(...)):
    job_id = run_gst(source, file_name)
    return {"job_id": job_id, "status": "running"}

@app.get("/extract/provisions")
def provisions_endpoint(source: str = Query(...), file_name: str = Query(...)):
    job_id = run_provisions(source, file_name)
    return {"job_id": job_id, "status": "running"}

@app.get("/extract/ledger")
def ledger_endpoint(source: str = Query(...), file_name: str = Query(...)):
    job_id = run_ledger(source, file_name)
    return {"job_id": job_id, "status": "running"}

@app.get("/extract/tds")
def tds_endpoint(source: str = Query(...), deducted_table: str = Query(...), paid_table: str = Query(...)):
    job_id = run_tds(source, deducted_table, paid_table)
    return {"job_id": job_id, "status": "running"}

@app.get("/extract/bills")
def bills_endpoint(source: str = Query(...), file_name: str = Query(...)):
    job_id = run_bills(source, file_name)
    return {"job_id": job_id, "status": "running"}

@app.get("/extract/stock")
def stock_endpoint(source: str = Query(...), file_name: str = Query(...)):
    job_id = run_stock(source, file_name)
    return {"job_id": job_id, "status": "running"}
