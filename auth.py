"""
auth.py — API Key authentication for Tally XML Extractor API

Usage:
    from auth import require_api_key

    @app.get("/some-endpoint")
    def my_endpoint(api_key: str = Depends(require_api_key)):
        ...

API key is read from GCP Secret Manager under the secret name "API_KEY".
Clients pass the key via the HTTP header:
    X-API-Key: <your-key>
"""

import secrets
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from gcp_secrets import get_secret


# ── Header scheme ─────────────────────────────────────────────────────────────
API_KEY_HEADER = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    description="Pass your API key in the **X-API-Key** header.",
)


# ── Load the valid key once at import time ────────────────────────────────────
def _load_api_key() -> str:
    key = get_secret("API_KEY")
    if not key or not key.strip():
        raise RuntimeError(
            "API_KEY is not set in GCP Secret Manager. "
            "Add a secret named 'API_KEY' and grant the service account access."
        )
    return key.strip()


_VALID_API_KEY: str = _load_api_key()


# ── Dependency ────────────────────────────────────────────────────────────────
async def require_api_key(
    api_key: str | None = Security(API_KEY_HEADER),
) -> str:
    """
    FastAPI dependency. Raises HTTP 401 if the key is missing or wrong.
    Use ``Depends(require_api_key)`` on any endpoint or router.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key. Supply it in the X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # constant-time comparison prevents timing attacks
    if not secrets.compare_digest(api_key.strip(), _VALID_API_KEY):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key.",
        )

    return api_key
