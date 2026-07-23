import re
import io
from sqlalchemy import create_engine
import pandas as pd
import os
from typing import Optional, Generator
from lxml import etree
from datetime import datetime
from gcp_secrets import get_secret
from dotenv import load_dotenv

from google.cloud import storage as gcs_storage

# Inlined from Stock.py — avoids a blocking import (Stock.py hangs on load)
def get_text(element, tag_name: str) -> str:
    try:
        tag = element.find(".//" + tag_name)
        if tag is not None and tag.text:
            return tag.text.strip()
    except Exception:
        pass
    return ""

load_dotenv()



DB_HOST = get_secret("DB_HOST")
DB_PORT = get_secret("DB_PORT")
DB_NAME = get_secret("DB_NAME")
DB_USER = get_secret("DB_USER")
DB_PASSWORD = get_secret("DB_PASSWORD")
BUCKET_NAME = get_secret("BUCKET_NAME")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def _get_engine():
    return create_engine(DATABASE_URL)


def upload_large_xml(bucket_name, local_file, destination_blob):
    client = gcs_storage.Client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(destination_blob)
    blob.chunk_size = 10 * 1024 * 1024
    with open(local_file, "rb") as f:
        blob.upload_from_file(f)
    return f"gs://{bucket_name}/{destination_blob}"


def list_gcs_files(bucket_name: str, prefix: Optional[str] = None) -> list:
    client = gcs_storage.Client()
    bucket = client.bucket(bucket_name)
    blobs  = bucket.list_blobs(prefix=prefix)
    return [f"gs://{bucket_name}/{blob.name}" for blob in blobs]


def list_gcs_folders(bucket_name: str, prefix: str = ""):
    client = gcs_storage.Client()
    blobs  = client.list_blobs(bucket_name, prefix=prefix, delimiter="/")
    list(blobs)
    return list(blobs.prefixes)


# ── defined ONCE ───────────────────────────────────────────────────────────────
def _parse_gcs_uri(uri: str):
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {uri}")
    without_scheme = uri[5:]
    bucket, _, blob = without_scheme.partition("/")
    if not bucket or not blob:
        raise ValueError(f"Invalid GCS URI: {uri}")
    return bucket, blob


def upload_file_to_gcs(local_path: str, bucket_name: str, destination_blob_name: str):
    client = gcs_storage.Client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket_name}/{destination_blob_name}"


# =========================================================
# TRUE STREAMING: GCS → clean bytes, never held in RAM
# =========================================================
_CLEAN_RE = [
    (re.compile(r'&#\d+;'),                  ''),
    (re.compile(r'&#x[0-9A-Fa-f]+;'),        ''),
    (re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]'), ''),
    (re.compile(r'[^\x09\x0A\x0D\x20-\xFF]'),''),
    (re.compile(r'&(?!amp;|lt;|gt;|quot;|apos;)'), '&amp;'),
]

def _clean_line(text: str) -> bytes:
    # skip regex on clean lines — 90% of lines will skip entirely
    if '&' not in text and '\x00' not in text and '\x0B' not in text:
        return text.encode("utf-8")
    for pattern, repl in _CLEAN_RE:
        text = pattern.sub(repl, text)
    return text.encode("utf-8")


def _gcs_clean_stream(bucket_name: str, blob_name: str) -> Generator[bytes, None, None]:
    """
    TRUE streaming generator — yields cleaned UTF-8 bytes line by line
    directly from GCS.  At most ONE line is ever in RAM at a time.
    No BytesIO accumulation, no temp file.
    """
    client = gcs_storage.Client()
    blob   = client.bucket(bucket_name).blob(blob_name)
    first  = True
    with blob.open("rb") as gcs_stream:
        for raw_line in gcs_stream:
            text = raw_line.decode("utf-8", errors="ignore")
            if first:
                idx = text.find("<")
                if idx > 0:
                    text = text[idx:]
                first = False
            yield _clean_line(text)


class _GCSStream:
    """
    File-like wrapper around the generator so lxml's iterparse can consume it
    directly without buffering the whole file.
    lxml calls read(size) — we satisfy that from a small internal buffer
    that refills from the generator one line at a time.
    """
    def __init__(self, gen: Generator[bytes, None, None]):
        self._gen  = gen
        self._buf  = b""

    def read(self, size: int = -1) -> bytes:
        if size == -1:
            return b"".join(self._gen)
        while len(self._buf) < size:
            try:
                self._buf += next(self._gen)
            except StopIteration:
                break
        chunk, self._buf = self._buf[:size], self._buf[size:]
        return chunk


def _open_gcs_stream(source: str) -> "_GCSStream":
    """
    Returns a file-like object that streams cleaned XML bytes from GCS.
    Pass directly to lxml iterparse — zero RAM accumulation.
    """
    bucket, blob = _parse_gcs_uri(source)
    return _GCSStream(_gcs_clean_stream(bucket, blob))


# NOTE: get_xml_source is currently unused dead code. None of the
# extractors below need full-tree parsing anymore — extract_gst,
# extract_month_end_provisions, and extract_tds all use
# _open_gcs_stream + iterparse like everything else in this module.
# Kept only as a utility in case a future extractor needs a full
# in-memory tree; remove if it stays unused.
def get_xml_source(source: str) -> io.BytesIO:
    """
    Compatibility helper for full-tree parsers.
    Streams from GCS, cleans, and returns a BytesIO.
    For large files prefer _open_gcs_stream + iterparse.
    """
    bucket, blob = _parse_gcs_uri(source)
    buf = io.BytesIO()
    for chunk in _gcs_clean_stream(bucket, blob):
        buf.write(chunk)
    buf.seek(0)
    return buf


# =========================================================
# SHARED HELPERS
# =========================================================
def safe_float(value) -> float:
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return 0.0


def parse_date(date_str: str):
    if not date_str:
        return None
    date_str = str(date_str).strip()
    for fmt in ("%Y%m%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except Exception:
            pass
    return None


def _save_to_db(df: pd.DataFrame, table_name: str):
    engine = _get_engine()
    df.to_sql(table_name, engine, if_exists="replace", index=False)

def _safe_records(df: pd.DataFrame) -> list:
    return df.where(pd.notnull(df), None).to_dict(orient="records")


def _release_voucher(voucher):
    """
    Shared cleanup for VOUCHER-level iterparse loops.
    voucher.clear() empties the element's children/text, but the now-empty
    element stays attached to its parent, and earlier emptied siblings are
    never detached — on a 1.5GB file with hundreds of thousands of vouchers
    that skeleton adds up. This detaches everything before the current
    element so the tree stays bounded.
    """
    voucher.clear()
    parent = voucher.getparent()
    if parent is not None:
        while voucher.getprevious() is not None:
            del parent[0]


# =========================================================
# BRS — iterparse, true streaming (VOUCHER-level clear)
# =========================================================
def _is_valid_bank_allocation(bank) -> bool:
    values = [
        bank.findtext("AMOUNT", "").strip(),
        bank.findtext("BANKERSDATE", "").strip(),
        bank.findtext("BANKDATE", "").strip(),
        bank.findtext("INSTRUMENTNUMBER", "").strip(),
        bank.findtext("UNIQUEREFERENCENUMBER", "").strip(),
    ]
    return any(values)


def _get_reconciliation_status(bank) -> str:
    if (bank.findtext("BANKERSDATE") or "").strip():
        return "Cleared"
    if (bank.findtext("BANKDATE") or "").strip():
        return "Likely Cleared"
    return "Uncleared"


def _build_brs_row(bank, voucher):
    """
    bank    — a BANKALLOCATIONS.LIST element
    voucher — the enclosing VOUCHER element (passed in directly,
              no getparent()/ancestor-walk needed since we no longer
              rely on uncleaned ancestors staying in memory)
    """
    if not _is_valid_bank_allocation(bank):
        return None

    ledger_entry = bank.getparent()  # direct parent, safe: voucher subtree is intact until voucher.clear()
    ledger_name  = ""
    if ledger_entry is not None:
        ledger_name = ledger_entry.findtext("LEDGERNAME", "").strip()

    voucher_type = (
        voucher.get("VCHTYPE", "") or voucher.findtext("VOUCHERTYPENAME", "") or ""
    ).strip()
    voucher_number = (
        voucher.findtext("VOUCHERNUMBER", "") or voucher.findtext("REFERENCE", "") or ""
    ).strip()
    voucher_date = (voucher.findtext("DATE", "") or "").strip()
    GUID = (voucher.findtext("GUID", "") or "").strip()

    amount_str = (bank.findtext("AMOUNT") or "0").strip()
    amount_val = safe_float(amount_str)
    abs_amount = abs(amount_val)

    return {
        "GUID":              GUID,
        "Ledger Name":       ledger_name,
        "Voucher Type":      voucher_type,
        "Voucher Number":    voucher_number,
        "Voucher Date":      voucher_date,
        "Instrument Date":   (bank.findtext("INSTRUMENTDATE") or "").strip(),
        "Bankers Date":      (bank.findtext("BANKERSDATE") or "").strip(),
        "Bank Date":         (bank.findtext("BANKDATE") or "").strip(),
        "Transaction Type":  (bank.findtext("TRANSACTIONTYPE") or "").strip(),
        "Payment Favouring": (bank.findtext("PAYMENTFAVOURING") or "").strip(),
        "Reference Number":  (
            bank.findtext("UNIQUEREFERENCENUMBER")
            or bank.findtext("UTRNUMBER")
            or bank.findtext("INSTRUMENTNUMBER")
            or bank.findtext("NAME")
            or ""
        ).strip(),
        "Payment Mode":      (bank.findtext("PAYMENTMODE") or "").strip(),
        "Bank Party Name":   (bank.findtext("BANKPARTYNAME") or "").strip(),
        "Amount":            amount_str,
        "Absolute Amount":   abs_amount,
        "Dr/Cr":             "Payment" if amount_val < 0 else "Receipt",
        "Final Status":      _get_reconciliation_status(bank),
    }


def extract_brs(source: str, file_name: Optional[str] = None) -> list:
    """
    TRUE streaming — iterparse direct from GCS, clears at VOUCHER level
    so the whole subtree (bank allocations + ledger entries) is freed
    together. Memory stays bounded on very large (1GB+) files.
    """
    stream = _open_gcs_stream(source)
    rows   = []

    context = etree.iterparse(
        stream, events=("end",), tag="VOUCHER", recover=True, huge_tree=True
    )
    for _, voucher in context:
        try:
            for bank in voucher.findall(".//BANKALLOCATIONS.LIST"):
                row = _build_brs_row(bank, voucher)
                if row:
                    rows.append(row)
        except Exception as e:
            print("BRS row error:", repr(e))
        finally:
            _release_voucher(voucher)

    df = pd.DataFrame(rows)
    if df.empty:
        return []

    string_cols = ["Ledger Name", "Voucher Type", "Voucher Number",
                   "Transaction Type", "Payment Favouring", "Reference Number",
                   "Payment Mode", "Bank Party Name"]
    df[string_cols] = df[string_cols].apply(lambda col: col.astype(str).str.strip())
    df[["Absolute Amount"]] = df[["Absolute Amount"]].apply(pd.to_numeric, errors="coerce").fillna(0)
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
    for col in ["Voucher Date", "Instrument Date", "Bankers Date", "Bank Date"]:
        df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce").dt.date

    if file_name:
        _save_to_db(df, file_name)
    return df.to_dict(orient="records")


# =========================================================
# GST — iterparse, true streaming
# =========================================================
def _get_text(element, tag_name: str) -> str:
    try:
        tag = element.find(".//" + tag_name)
        if tag is not None and tag.text:
            return tag.text.strip()
    except Exception:
        pass
    return ""


def _extract_tax_amounts(voucher) -> dict:
    taxes = {"CGST": 0.0, "SGST": 0.0, "IGST": 0.0, "CESS": 0.0}
    for entry in voucher.findall(".//LEDGERENTRIES.LIST"):
        ledger_name = _get_text(entry, "LEDGERNAME").upper()
        if not any(k in ledger_name for k in ("CGST", "SGST", "IGST", "CESS")):
            continue
        amount_tag = entry.find("AMOUNT")
        if amount_tag is None:
            continue
        try:
            amount = float(amount_tag.text.strip())
        except Exception:
            continue
        if amount == 0:
            continue
        is_deemed = _get_text(entry, "ISDEEMEDPOSITIVE").upper()
        if is_deemed == "YES" and amount < 0:
            abs_amount = abs(amount)
        elif is_deemed == "NO" and amount > 0:
            abs_amount = amount
        else:
            continue
        if "CGST" in ledger_name:
            taxes["CGST"] += abs_amount
        elif "SGST" in ledger_name:
            taxes["SGST"] += abs_amount
        elif "IGST" in ledger_name:
            taxes["IGST"] += abs_amount
        elif "CESS" in ledger_name:
            taxes["CESS"] += abs_amount
    return taxes


def _extract_invoice_amount(voucher, party_name: str) -> float:
    TAX_KEYWORDS = ("CGST", "SGST", "IGST", "CESS", "TAX", "DISCOUNT")
    if party_name:
        for entry in voucher.findall(".//LEDGERENTRIES.LIST"):
            if _get_text(entry, "LEDGERNAME").upper() == party_name.upper():
                try:
                    amt = entry.find("AMOUNT")
                    if amt is not None and amt.text:
                        return abs(float(amt.text.strip()))
                except Exception:
                    pass
                break
    best = 0.0
    for entry in voucher.findall(".//LEDGERENTRIES.LIST"):
        name = _get_text(entry, "LEDGERNAME").upper()
        if not name or any(kw in name for kw in TAX_KEYWORDS):
            continue
        amt_tag = entry.find("AMOUNT")
        if amt_tag is None:
            continue
        try:
            amt = abs(float(amt_tag.text.strip()))
            if amt > best:
                best = amt
        except Exception:
            continue
    if best > 0:
        return best
    try:
        return abs(float(_get_text(voucher, "AMOUNT")))
    except Exception:
        return 0.0


def extract_gst(source: str, file_name: Optional[str] = None) -> list:
    """TRUE streaming — iterparse direct from GCS."""
    stream  = _open_gcs_stream(source)
    rows    = []

    context = etree.iterparse(stream, events=("end",), tag="VOUCHER",
                               recover=True, huge_tree=True)
    for _, voucher in context:
        try:
            tax_data  = _extract_tax_amounts(voucher)
            total_tax = sum(tax_data.values())
            if total_tax == 0:
                continue

            party_name     = _get_text(voucher, "PARTYLEDGERNAME")
            invoice_amount = _extract_invoice_amount(voucher, party_name)
            voucher_date   = _get_text(voucher, "DATE")
            try:
                parsed_date = pd.to_datetime(voucher_date, format="%Y%m%d").date()
            except Exception:
                parsed_date = None

            rows.append({
                "GUID":          (voucher.findtext("GUID", "") or "").strip(),
                "VoucherType":   _get_text(voucher, "VOUCHERTYPENAME"),
                "VoucherNumber": _get_text(voucher, "VOUCHERNUMBER"),
                "Date":          parsed_date,
                "PartyName":     party_name,
                "GSTIN":         _get_text(voucher, "PARTYGSTIN"),
                "PlaceOfSupply": _get_text(voucher, "PLACEOFSUPPLY"),
                "State":         _get_text(voucher, "STATENAME"),
                "TaxableValue":  round(invoice_amount - total_tax, 2),
                "CGST":          tax_data["CGST"],
                "SGST":          tax_data["SGST"],
                "IGST":          tax_data["IGST"],
                "CESS":          tax_data["CESS"],
                "InvoiceAmount": invoice_amount,
            })
        except Exception as e:
            print("GST row error:", repr(e))
        finally:
            _release_voucher(voucher)

    df = pd.DataFrame(rows)
    if df.empty:
        return []
    string_cols = ["GUID", "VoucherType", "VoucherNumber", "PartyName", "GSTIN", "PlaceOfSupply", "State"]
    df[string_cols] = df[string_cols].apply(lambda col: col.astype(str).str.strip())
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    int_cols = ["TaxableValue", "CGST", "SGST", "IGST", "CESS", "InvoiceAmount"]
    df[int_cols] = df[int_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    if file_name:
        _save_to_db(df, file_name)
    return df.to_dict(orient="records")


# =========================================================
# MONTH-END PROVISIONS — iterparse, true streaming
# =========================================================
PROVISION_KEYWORDS = [
    "provision", "payable", "outstanding", "accrued",
    "salary payable", "expense payable", "expenses payable",
    "tds payable", "gst payable", "pf payable", "esi payable",
    "pt payable", "professional tax payable", "outstanding expenses",
    "provision for expenses", "provision for salary", "provision for rent",
    "provision for audit", "provision for depreciation", "provision for bonus",
    "provision for gratuity", "provision for leave", "provision for bad debt",
    "provision for doubtful", "provision for tax", "provision for income tax",
    "audit fee payable", "audit fees payable", "bonus payable",
    "interest payable", "rent payable", "electricity payable",
    "telephone payable", "insurance payable", "repairs payable",
    "maintenance payable", "contractor payable", "legal fee payable",
    "consultancy payable",
]


def _is_provision_ledger(ledger_name: str) -> bool:
    ledger_name = " ".join(str(ledger_name).lower().strip().split())
    return any(kw in ledger_name for kw in PROVISION_KEYWORDS)


def _get_voucher_type(voucher) -> str:
    vch = (voucher.findtext("VCHTYPE", "") or "").strip()
    return vch or (voucher.get("VCHTYPE", "") or "").strip()


def _get_ledger_entries(voucher):
    entries = voucher.findall("ALLLEDGERENTRIES.LIST")
    return entries if entries else voucher.findall("LEDGERENTRIES.LIST")


def _get_ledger_name(ledger) -> str:
    name = (ledger.findtext("LEDGERNAME", "") or "").strip()
    return name or (ledger.findtext("PARTYLEDGERNAME", "") or "").strip()


def _get_nature(ledger, amount: float) -> str:
    flag = (ledger.findtext("ISDEEMEDPOSITIVE", "") or "").strip().lower()
    if flag == "yes":
        return "Debit"
    if flag == "no":
        return "Credit"
    return "Credit" if amount < 0 else "Debit"


def extract_month_end_provisions(source: str, file_name: Optional[str] = None) -> list:
    """TRUE streaming — iterparse direct from GCS."""
    stream  = _open_gcs_stream(source)
    rows    = []

    context = etree.iterparse(stream, events=("end",), tag="VOUCHER",
                               recover=True, huge_tree=True)
    for _, voucher in context:
        try:
            voucher_type = _get_voucher_type(voucher)
            GUID         = (voucher.findtext("GUID", "") or "").strip()
            date_raw     = (voucher.findtext("DATE", "") or "").strip()
            voucher_no   = (voucher.findtext("VOUCHERNUMBER", "") or "").strip()
            narration    = (voucher.findtext("NARRATION", "") or "").strip()
            fmt_date     = pd.to_datetime(date_raw, format="%Y%m%d", errors="coerce").date() if date_raw else None

            for ledger in _get_ledger_entries(voucher):
                ledger_name = _get_ledger_name(ledger)
                amount      = safe_float(ledger.findtext("AMOUNT", "0"))
                if not _is_provision_ledger(ledger_name):
                    continue
                rows.append({
                    "GUID":           GUID,
                    "Date":           fmt_date,
                    "Voucher Number": voucher_no,
                    "Voucher Type":   voucher_type,
                    "Ledger Name":    ledger_name,
                    "Amount":         abs(amount),
                    "Nature":         _get_nature(ledger, amount),
                    "Narration":      narration,
                })
        except Exception as e:
            print("Provisions row error:", repr(e))
        finally:
            _release_voucher(voucher)

    df = pd.DataFrame(rows, columns=[
        "GUID", "Date", "Voucher Number", "Voucher Type",
        "Ledger Name", "Amount", "Nature", "Narration",
    ])
    if df.empty:
        if file_name:
            _save_to_db(df, file_name)
        return []

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    string_cols = ["GUID", "Voucher Number", "Voucher Type", "Ledger Name", "Nature", "Narration"]
    df[string_cols] = df[string_cols].apply(lambda col: col.astype(str).str.strip())
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)

    if file_name:
        _save_to_db(df, file_name)
    return df.to_dict(orient="records")


# =========================================================
# LEDGER TRANSACTIONS — iterparse, true streaming
# =========================================================
def extract_ledger_transactions(source: str, file_name: Optional[str] = None) -> list:
    """TRUE streaming — iterparse direct from GCS."""
    stream  = _open_gcs_stream(source)
    rows    = []

    context = etree.iterparse(stream, events=("end",), tag="VOUCHER",
                               recover=True, huge_tree=True)
    for _, voucher in context:
        try:
            GUID         = (voucher.findtext("GUID", "") or "").strip()
            voucher_no   = (voucher.findtext("VOUCHERNUMBER", "") or "").strip()
            party        = (voucher.findtext("PARTYLEDGERNAME", "") or "").strip()
            if not party:
                party = (get_text(voucher, "BASICBASEPARTYNAME") or "").strip()
            voucher_type = (voucher.findtext("VOUCHERTYPENAME", "") or "").strip()
            if not voucher_type:
                voucher_type = (voucher.get("VCHTYPE", "") or "").strip()
            date         = (voucher.findtext("DATE", "") or "").strip()
            narration    = (voucher.findtext("NARRATION", "") or "").strip()

            ledgers = voucher.findall("ALLLEDGERENTRIES.LIST") or voucher.findall("LEDGERENTRIES.LIST")
            for ledger in ledgers:
                ledger_name = (ledger.findtext("LEDGERNAME", "") or "").strip()
                amount      = safe_float(ledger.findtext("AMOUNT", "0"))
                rows.append({
                    "GUID":         GUID,
                    "Date":         date,
                    "Voucher_No":   voucher_no,
                    "Voucher_Type": voucher_type,
                    "Narration":    narration,
                    "Party":        party,
                    "Ledger_Name":  ledger_name,
                    "Amount":       amount,
                    "Debit":        abs(amount) if amount < 0 else 0.0,
                    "Credit":       amount      if amount > 0 else 0.0,
                })
        except Exception as e:
            print("Ledger row error:", repr(e))
        finally:
            _release_voucher(voucher)

    df = pd.DataFrame(rows, columns=[
        "GUID", "Date", "Voucher_No", "Voucher_Type", "Narration",
        "Party", "Ledger_Name", "Amount", "Debit", "Credit",
    ])
    if df.empty:
        if file_name:
            _save_to_db(df, file_name)
        return []

    df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d", errors="coerce").dt.date
    string_cols = ["GUID", "Voucher_No", "Voucher_Type", "Narration", "Party", "Ledger_Name"]
    df[string_cols] = df[string_cols].apply(lambda col: col.astype(str).str.strip())
    df[["Amount", "Debit", "Credit"]] = df[["Amount", "Debit", "Credit"]].apply(
        pd.to_numeric, errors="coerce").fillna(0)

    if file_name:
        _save_to_db(df, file_name)
    return df.to_dict(orient="records")


# =========================================================
# TDS — iterparse, true streaming
# =========================================================
TDS_KEYWORDS = {
    "tds", "tds payable", "tds receivable", "tds liability",
    "tds on professional charges", "tds on professional fees",
    "professional fees tds", "tds on technical services", "technical service tds",
    "tds on contract", "contractor tds", "tds on contractor payment",
    "tds on transport", "transport contractor tds",
    "tds on commission", "commission tds", "tds on brokerage", "brokerage tds",
    "tds on interest", "interest tds",
    "tds on rent", "rent tds", "tds on machinery rent",
    "tds on salary", "salary tds",
    "tds on advertisement", "advertisement tds",
    "tds on property", "tds on immovable property",
    "tds on insurance", "insurance commission tds",
    "tds on purchase", "tds on purchase of goods",
    "tds on cash withdrawal", "tds on dividend",
    "tds on benefits", "tds on perquisites",
    "tds on vda", "tds on crypto",
    "tcs", "tcs payable", "tax collected at source",
    "192", "192b",
    "194a", "194b", "194bb", "194c", "194d", "194da",
    "194e", "194f", "194g", "194h", "194i", "194ia",
    "194ib", "194j", "194k", "194la", "194m", "194n",
    "194o", "194q", "194r", "194s",
    "206c", "206cr",
    "sec 194j", "sec 194c", "u/s 194j", "u/s 194c",
    "u/s 194i", "u/s 194h", "u/s 194a", "u/s 192",
    "sec 194q", "u/s 194q",
}
# NOTE: matching is substring-based (see _is_tds_ledger below). The bare
# numeric entries ("192", "194a", etc.) can false-positive against ledger
# names that happen to contain those digits (e.g. an invoice/bill number).
# Left as-is since tightening this is a business-logic call, not a
# streaming/memory fix — worth a word-boundary regex if false positives
# show up in practice.


def _is_tds_ledger(ledger_name: str) -> bool:
    lower = ledger_name.strip().lower()
    return lower in TDS_KEYWORDS or any(kw in lower for kw in TDS_KEYWORDS)


def _is_tds_by_xml_tags(ledger_elem) -> bool:
    return bool(
        ledger_elem.findtext(".//TDSAMOUNT")
        or ledger_elem.findtext(".//TDSRATE")
        or ledger_elem.findtext(".//TDSNATUREOFPAYMENT")
        or ledger_elem.findtext(".//TAXTYPE", "").lower() == "tds"
    )


def _safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def extract_tds(
    source:         str,
    deducted_table: Optional[str] = None,
    paid_table:     Optional[str] = None,
) -> dict:
    """TRUE streaming — iterparse direct from GCS."""
    stream         = _open_gcs_stream(source)
    deducted_rows: list = []
    payment_rows:  list = []

    context = etree.iterparse(stream, events=("end",), tag="VOUCHER",
                               recover=True, huge_tree=True)
    for _, voucher in context:
        try:
            GUID     = (voucher.findtext("GUID", "") or "").strip()
            raw_date = voucher.findtext("DATE", "")
            if not raw_date:
                continue
            try:
                voucher_date = datetime.strptime(raw_date, "%Y%m%d")
                month_key    = voucher_date.strftime("%Y-%m")
            except ValueError:
                continue

            party_name     = voucher.findtext("PARTYLEDGERNAME", "")
            voucher_number = voucher.findtext("VOUCHERNUMBER", "")
            voucher_type   = (voucher.findtext("VOUCHERTYPENAME", "") or "").strip()

            for ledger in voucher.findall(".//ALLLEDGERENTRIES.LIST"):
                ledger_name = ledger.findtext("LEDGERNAME", "").strip()
                if not (_is_tds_ledger(ledger_name) or _is_tds_by_xml_tags(ledger)):
                    continue

                raw_amount = ledger.findtext(".//TDSAMOUNT") or ledger.findtext("AMOUNT", "0")
                tds_amount = abs(_safe_float(raw_amount))
                is_debit   = _safe_float(ledger.findtext("AMOUNT", "0")) > 0
                tds_nature = (ledger.findtext(".//TDSNATUREOFPAYMENT") or "").strip()

                base_row = {
                    "GUID":           GUID,
                    "voucher_date":   voucher_date,
                    "month":          month_key,
                    "voucher_number": voucher_number,
                    "voucher_type":   voucher_type,
                    "party_name":     party_name,
                    "ledger_name":    ledger_name,
                    "tds_nature":     tds_nature,
                    "tds_amount":     tds_amount,
                }
                if not is_debit:
                    deducted_rows.append({**base_row, "entry_type": "Deducted"})
                else:
                    payment_rows.append({**base_row, "entry_type": "Paid to Govt"})
        except Exception as e:
            print("TDS row error:", repr(e))
        finally:
            _release_voucher(voucher)

    string_cols = ["GUID", "month", "voucher_number", "voucher_type",
                   "party_name", "ledger_name", "tds_nature", "entry_type"]
    date_cols   = ["voucher_date"]
    int_cols    = ["tds_amount"]

    def _finalize(rows):
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        df[int_cols]    = df[int_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        df[string_cols] = df[string_cols].apply(lambda col: col.astype(str).str.strip())
        return df

    deducted_df = _finalize(deducted_rows)
    payment_df  = _finalize(payment_rows)

    if deducted_table and not deducted_df.empty:
        _save_to_db(deducted_df, deducted_table)
    if paid_table and not payment_df.empty:
        _save_to_db(payment_df, paid_table)

    return {
        "tds_deducted": deducted_df.head(10).to_dict(orient="records"),
        "tds_paid":     payment_df.head(10).to_dict(orient="records"),
    }


# =========================================================
# BILLS — iterparse, true streaming
# =========================================================
def read_tally_xml(source: str) -> pd.DataFrame:
    stream  = _open_gcs_stream(source)
    rows    = []

    context = etree.iterparse(stream, events=("end",), tag="VOUCHER",
                               recover=True, huge_tree=True)
    for _, voucher in context:
        try:
            guid         = (voucher.findtext("GUID", "") or "").strip()
            voucher_no   = (voucher.findtext("VOUCHERNUMBER", "") or "").strip()
            voucher_type = (voucher.findtext("VOUCHERTYPENAME", "") or "").strip()
            date         = (voucher.findtext("DATE", "") or "").strip()
            party_ledger = (voucher.findtext("PARTYLEDGERNAME", "") or "").strip()

            ledger_entries = (
                voucher.findall(".//LEDGERENTRIES.LIST")
                + voucher.findall(".//ALLLEDGERENTRIES.LIST")
            )
            for ledger_entry in ledger_entries:
                ledger_name = (ledger_entry.findtext("LEDGERNAME") or "").strip()
                try:
                    amount = float(ledger_entry.findtext("AMOUNT") or 0)
                except Exception:
                    amount = 0.0

                for bill in ledger_entry.findall(".//BILLALLOCATIONS.LIST"):
                    bill_name = (bill.findtext("NAME") or "").strip()
                    bill_type = (bill.findtext("BILLTYPE") or "").strip()
                    try:
                        bill_amount = float(bill.findtext("AMOUNT") or 0)
                    except Exception:
                        bill_amount = 0.0
                    rows.append({
                        "GUID":           guid,
                        "Voucher Number": voucher_no,
                        "Voucher Type":   voucher_type,
                        "Date":           date,
                        "Party Ledger":   party_ledger,
                        "Ledger Name":    ledger_name,
                        "Bill Name":      bill_name,
                        "Bill Type":      bill_type,
                        "Amount":         bill_amount,
                    })
        except Exception as e:
            print("Bills voucher error:", e)
        finally:
            _release_voucher(voucher)

    expected_columns = [
        "GUID", "Voucher Number", "Voucher Type", "Date",
        "Party Ledger", "Ledger Name", "Bill Name", "Bill Type", "Amount"
    ]
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=expected_columns)
    df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d", errors="coerce").dt.date
    string_cols = ["GUID", "Voucher Number", "Voucher Type", "Party Ledger",
                   "Ledger Name", "Bill Name", "Bill Type"]
    df[string_cols] = df[string_cols].apply(lambda col: col.astype(str).str.strip())
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
    for col in expected_columns:
        if col not in df.columns:
            df[col] = ""
    return df[expected_columns]


def build_outstanding(df: pd.DataFrame) -> pd.DataFrame:
    temp = df.copy()
    temp["Bill Type"]    = temp["Bill Type"].astype(str).str.strip().str.upper()
    temp["Voucher Type"] = temp["Voucher Type"].astype(str).str.strip().str.upper()
    temp["Amount"]       = pd.to_numeric(temp["Amount"], errors="coerce").fillna(0)
    temp["Date"]         = pd.to_datetime(temp["Date"], errors="coerce").dt.date

    new_ref_df  = temp[temp["Bill Type"] == "NEW REF"].copy()
    agst_ref_df = temp[temp["Bill Type"] == "AGST REF"].copy()
    final_rows  = []

    for _, new_row in new_ref_df.iterrows():
        matching_agst = agst_ref_df[
            (agst_ref_df["Ledger Name"] == new_row["Ledger Name"])
            & (agst_ref_df["Bill Name"] == new_row["Bill Name"])
        ].sort_values("Date")

        new_amount     = new_row["Amount"]
        invoice_amount = abs(new_amount)

        if matching_agst.empty:
            final_rows.append({
                "New Bill ID":       new_row["GUID"],
                "Against Bill ID":   "",
                "Ledger Name":       new_row["Ledger Name"],
                "Bill Name":         new_row["Bill Name"],
                "New Bill Date":     new_row["Date"],
                "Against Bill Date": "",
                "Days Difference":   0,
                "Invoice Amount":    invoice_amount,
                "Adjusted Amount":   0,
                "Outstanding":       round(invoice_amount, 2),
            })
            continue

        running_balance = invoice_amount
        for _, agst_row in matching_agst.iterrows():
            days_diff = (agst_row["Date"] - new_row["Date"]).days
            same_sign = (
                (agst_row["Amount"] < 0 and new_amount < 0)
                or (agst_row["Amount"] > 0 and new_amount > 0)
            )
            adj_display = (
                -abs(agst_row["Amount"])
                if agst_row["Voucher Type"] == "JOURNAL" and same_sign
                else abs(agst_row["Amount"])
            )
            running_balance = round(running_balance - adj_display, 2)
            final_rows.append({
                "New Bill ID":          new_row["GUID"],
                "Against Bill ID":      agst_row["GUID"],
                "Ledger Name":          new_row["Ledger Name"],
                "Bill Name":            new_row["Bill Name"],
                "New Bill Date":        new_row["Date"],
                "Against Bill Date":    agst_row["Date"],
                "Days Difference":      days_diff,
                "Voucher Type (Agst)":  agst_row["Voucher Type"],
                "Invoice Amount":       invoice_amount,
                "Adjusted Amount":      adj_display,
                "Outstanding":          running_balance,
            })

    result = pd.DataFrame(final_rows)
    if not result.empty:
        string_cols = ["New Bill ID", "Against Bill ID", "Ledger Name", "Bill Name"]
        if "Voucher Type (Agst)" in result.columns:
            string_cols.append("Voucher Type (Agst)")
        result[string_cols] = result[string_cols].apply(lambda col: col.astype(str).str.strip())
        int_cols = ["Days Difference", "Invoice Amount", "Adjusted Amount", "Outstanding"]
        result[int_cols] = result[int_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        for col in ["New Bill Date", "Against Bill Date"]:
            result[col] = pd.to_datetime(result[col], errors="coerce").dt.date
    return result


def extract_bills(source: str, file_name: Optional[str] = None) -> dict:
    df          = read_tally_xml(source)
    outstanding = build_outstanding(df)

    bill_types = {}
    for bill_type in df["Bill Type"].dropna().astype(str).str.strip().unique():
        if not bill_type:
            continue
        subset = df[df["Bill Type"].astype(str).str.strip() == bill_type]
        if file_name and not subset.empty:
            _save_to_db(subset, f"{file_name}_{bill_type.lower().replace(' ', '_')}")
        bill_types[bill_type] = subset.head(10).to_dict(orient="records")

    if file_name and not outstanding.empty:
        _save_to_db(outstanding, f"{file_name}_outstanding")

    return {
        "bill_types":  bill_types,
        "outstanding": outstanding.head(10).to_dict(orient="records"),
    }


# =========================================================
# STOCK — iterparse, true streaming
# =========================================================
INVENTORY_TAGS = [
    "ALLINVENTORYENTRIES.LIST",
    "INVENTORYENTRIES.LIST",
    "INVENTORYENTRIESIN.LIST",
    "INVENTORYENTRIESOUT.LIST",
    "INVENTORYENTRIESWITHOUTSTOCK.LIST",
]


def _get_inventory_entries(voucher):
    entries = []
    for tag in INVENTORY_TAGS:
        entries.extend(voucher.findall(f".//{tag}"))
    return entries


def extract_stock(source: str, file_name: Optional[str] = None) -> list:
    """TRUE streaming — iterparse direct from GCS."""
    stream  = _open_gcs_stream(source)
    rows    = []

    context = etree.iterparse(stream, events=("end",), tag="VOUCHER",
                               recover=True, huge_tree=True)
    for _, voucher in context:
        try:
            guid         = (voucher.findtext("GUID", "") or "").strip()
            voucher_no   = (voucher.findtext("VOUCHERNUMBER", "") or "").strip()
            voucher_type = (voucher.findtext("VOUCHERTYPENAME", "") or "").strip()
            date         = (voucher.findtext("DATE", "") or "").strip()
            party_name   = (voucher.findtext("PARTYLEDGERNAME", "") or "").strip()

            for item in _get_inventory_entries(voucher):
                stock_item = (item.findtext("STOCKITEMNAME") or "").strip()
                if not stock_item:
                    continue
                rows.append({
                    "GUID":          guid,
                    "Date":          date,
                    "VoucherType":   voucher_type,
                    "VoucherNumber": voucher_no,
                    "PartyName":     party_name,
                    "StockItem":     stock_item,
                    "ActualQty":     (item.findtext("ACTUALQTY") or "").strip(),
                    "BilledQty":     (item.findtext("BILLEDQTY") or "").strip(),
                    "Rate":          (item.findtext("RATE") or "").strip(),
                    "Amount":        (item.findtext("AMOUNT") or "").strip(),
                })
        except Exception as e:
            print("Stock row error:", repr(e))
        finally:
            _release_voucher(voucher)

    expected_columns = [
        "GUID", "Date", "VoucherType", "VoucherNumber", "PartyName",
        "StockItem", "ActualQty", "BilledQty", "Rate", "Amount",
    ]
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=expected_columns)
    df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d", errors="coerce").dt.date
    string_cols = ["GUID", "VoucherType", "VoucherNumber", "PartyName",
                   "StockItem", "ActualQty", "BilledQty", "Rate"]
    df[string_cols] = df[string_cols].apply(lambda col: col.astype(str).str.strip())
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
    for col in expected_columns:
        if col not in df.columns:
            df[col] = ""
    df = df[expected_columns].reset_index(drop=True)

    if file_name:
        _save_to_db(df, file_name)
    return df.to_dict(orient="records")



