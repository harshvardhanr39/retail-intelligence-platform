from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from app.routers import revenue, inventory, products, pipeline
from app.db.connection import check_db_connection
from datetime import datetime
import os

app = FastAPI(
    title="Retail Intelligence API",
    version="1.0.0",
    docs_url="/docs",
)

# ── CORS must be added FIRST before any routers ───────────
origins = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:3000"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # allow all origins temporarily
    allow_credentials=False,       # must be False when allow_origins=["*"]
    allow_methods=["*"],           # allow all methods including OPTIONS
    allow_headers=["*"],
)

# ── Prometheus ────────────────────────────────────────────
Instrumentator().instrument(app).expose(app)

# ── Routers ───────────────────────────────────────────────
app.include_router(revenue.router,   prefix="/api/v1")
app.include_router(inventory.router, prefix="/api/v1")
app.include_router(products.router,  prefix="/api/v1")
app.include_router(pipeline.router,  prefix="/api/v1")

# ── Health ────────────────────────────────────────────────
@app.get("/api/v1/health", tags=["Health"])
def health_check():
    db_ok = check_db_connection()
    return {
        "status":    "healthy" if db_ok else "degraded",
        "database":  "connected" if db_ok else "disconnected",
        "timestamp": datetime.utcnow().isoformat(),
    }

@app.get("/", tags=["Health"])
def root():
    return {"message": "Retail Intelligence API", "docs": "/docs"}
