from typing import Optional


from fastapi import FastAPI, Query, UploadFile, File, Form 



from tally_extractor import (
    extract_bills,
    extract_brs,
    extract_gst,
    extract_month_end_provisions,
    extract_ledger_transactions,
    extract_tds,
    list_gcs_files,
    BUCKET_NAME,
    list_gcs_folders,
    extract_stock,
  
    

    upload_file_to_gcs
)

print("1. After importing tally_extractor")

app = FastAPI(
    title="Tally XML Extractor API",
    description="Reads Tally XML exports from GCS and extracts BRS, GST, provisions, or ledger transactions.",
    version="2.0.0",
)



@app.get("/List_folders")
def list_folders():
    """
    Lists all folders in the GCS bucket.
    """
    return list_gcs_folders(BUCKET_NAME)    

@app.post("/upload_gcs")
async def upload_gcs_file(
    file: UploadFile = File(..., description="File to upload."),
    bucket_name: str = Form(..., description="GCS bucket name."),
    destination_blob_name: str = Form(..., description="Destination blob name in GCS."),
):
    gcs_uri = await upload_from_request(file, bucket_name, destination_blob_name)
    return {"gcs_uri": gcs_uri}

    
):
    """
    Uploads a local file to Google Cloud Storage.
    """
    return upload_file_to_gcs(local_path, BUCKET_NAME, destination_blob_name)

# =========================================================
# LIST FILES
# =========================================================
@app.get("/files", tags=["files"])
def list_files(
    prefix: Optional[str] = Query(
        None,
        description="Folder prefix (e.g. uploads_xml/)"
    )
):
    """
    Lists all files in the GCS bucket.
    """
    return list_gcs_files(BUCKET_NAME, prefix)
# =========================================================
# ENDPOINTS
# =========================================================
@app.get("/extract/brs", tags=["extraction"])
def brs_endpoint(
    source:    str           = Query(..., description="GCS URI to the Tally XML export. e.g. gs://bucket/file.xml"),
    file_name: Optional[str] = Query(None, description="If given, saves to PostgreSQL as this table name."),
):
    return extract_brs(source, file_name)


@app.get("/extract/gst", tags=["extraction"])
def gst_endpoint(
    source:    str           = Query(..., description="GCS URI to the Tally XML export. e.g. gs://bucket/file.xml"),
    file_name: Optional[str] = Query(None, description="If given, saves to PostgreSQL as this table name."),
):
    return extract_gst(source, file_name)


@app.get("/extract/provisions", tags=["extraction"])
def provisions_endpoint(
    source:    str           = Query(..., description="GCS URI to the Tally XML export. e.g. gs://bucket/file.xml"),
    file_name: Optional[str] = Query(None, description="If given, saves to PostgreSQL as this table name."),
):
    return extract_month_end_provisions(source, file_name)


@app.get("/extract/ledger", tags=["extraction"])
def ledger_endpoint(
    source:    str           = Query(..., description="GCS URI to the Tally XML export. e.g. gs://bucket/file.xml"),
    file_name: Optional[str] = Query(None, description="If given, saves to PostgreSQL as this table name."),
):
    return extract_ledger_transactions(source, file_name)

@app.get("/extract/tds", tags=["extraction"])
def tds_endpoint(
    source:          str           = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    deducted_table:  Optional[str] = Query(None, description="If given, saves TDS Deducted to PostgreSQL as this table name."),
    paid_table:      Optional[str] = Query(None, description="If given, saves TDS Paid to PostgreSQL as this table name."),
):
    """
    Returns two DataFrames (top 10 rows each):
    - tds_deducted : TDS liability created (credit entries)
    - tds_paid     : TDS paid to government (debit entries)
    """
    return extract_tds(source, deducted_table, paid_table)

 
@app.get("/extract/bills", tags=["extraction"])
def bills_endpoint(
    source:    str           = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: Optional[str] = Query(None, description="If given, saves outstanding to PostgreSQL as this table name."),
):
    """
    Returns:
    - bill_types  : each bill type (New Ref, Agst Ref, etc.) with top 10 rows
    - outstanding : top 10 outstanding rows
    No 'All Bills' included.
    """
    return extract_bills(source, file_name)
 

@app.get("/extract/stock", tags=["extraction"])
def stock_endpoint( 
    source:    str           = Query(..., description="GCS URI e.g. gs://bucket/file.xml"),
    file_name: Optional[str] = Query(None, description="If given, saves stock to PostgreSQL as this table name."),
):
    """
    Returns:
    - stock : top 10 stock rows
    """
    return extract_stock(source, file_name)
