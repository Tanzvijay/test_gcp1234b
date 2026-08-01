import os
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from routers import app  # ✅ IMPORT app from routers.py

# ✅ ADD MIDDLEWARE
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ RUN UVICORN
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
