import os
import uvicorn




app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Add your frontend URL instead of "*" in production
    
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
