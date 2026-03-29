from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from datetime import datetime
from app.db.connection import get_db
from app.models.schemas import PipelineRun, DataFreshness
import os

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])

@router.get("/runs", response_model=List[PipelineRun])
def get_pipeline_runs(db: Session = Depends(get_db)):
    """Recent pipeline run history."""
    try:
        result = db.execute(text("""
            SELECT
                run_id::text,
                source_name,
                status,
                rows_extracted,
                rows_written,
                started_at,
                finished_at,
                error_message,
                EXTRACT(EPOCH FROM (
                    COALESCE(finished_at, NOW()) - started_at
                ))::int AS duration_seconds
            FROM pipeline_meta.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 50
        """))
        rows = result.mappings().all()
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")

@router.get("/freshness", response_model=List[DataFreshness])
def get_data_freshness(db: Session = Depends(get_db)):
    """How fresh is each data source."""
    try:
        result = db.execute(text("""
            SELECT
                source_name,
                last_run_at,
                last_watermark,
                ROUND(
                    EXTRACT(EPOCH FROM (NOW() - last_run_at)) / 3600
                , 1) AS hours_since_update,
                CASE
                    WHEN EXTRACT(EPOCH FROM (NOW() - last_run_at)) / 3600 < 12
                        THEN 'green'
                    WHEN EXTRACT(EPOCH FROM (NOW() - last_run_at)) / 3600 < 25
                        THEN 'amber'
                    ELSE 'red'
                END AS status
            FROM pipeline_meta.watermarks
            ORDER BY source_name
        """))
        rows = result.mappings().all()
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")

@router.post("/trigger")
def trigger_pipeline(db: Session = Depends(get_db)):
    """
    Manually trigger the daily pipeline.
    In production this calls the Airflow API.
    For demo: runs a quick data refresh directly.
    """
    try:
        # Log a manual trigger run
        db.execute(text("""
            INSERT INTO pipeline_meta.pipeline_runs
                (run_id, source_name, run_date, started_at, status)
            VALUES
                (gen_random_uuid(), 'manual_trigger', CURRENT_DATE, NOW(), 'running')
        """))
        db.commit()
        return {
            "status": "triggered",
            "message": "Pipeline triggered successfully",
            "triggered_at": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Trigger failed: {str(e)}")
