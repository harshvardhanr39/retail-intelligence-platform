from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from datetime import datetime, date
from app.db.connection import get_db
from app.models.schemas import DailyRevenue, RevenueSummary, CategoryPerformance

router = APIRouter(prefix="/revenue", tags=["Revenue"])

@router.get("/daily", response_model=List[DailyRevenue])
def get_daily_revenue(
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db)
):
    """Daily revenue metrics for the last N days."""
    try:
        result = db.execute(text("""
            SELECT
                date,
                total_revenue,
                order_count,
                avg_order_value,
                revenue_7day_avg,
                prev_day_revenue,
                revenue_growth_pct
            FROM gold.mart_daily_revenue
            WHERE date >= CURRENT_DATE - INTERVAL ':days days'
            ORDER BY date DESC
        """), {"days": days})
        rows = result.mappings().all()
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")

@router.get("/summary", response_model=RevenueSummary)
def get_revenue_summary(db: Session = Depends(get_db)):
    """Month-to-date revenue summary with growth vs previous month."""
    try:
        result = db.execute(text("""
            WITH current_month AS (
                SELECT
                    COALESCE(SUM(total_revenue), 0)   AS revenue_mtd,
                    COALESCE(SUM(order_count), 0)      AS orders_mtd,
                    COALESCE(AVG(avg_order_value), 0)  AS aov
                FROM gold.mart_daily_revenue
                WHERE DATE_TRUNC('month', date) = DATE_TRUNC('month', CURRENT_DATE)
            ),
            prev_month AS (
                SELECT COALESCE(SUM(total_revenue), 0) AS revenue_prev
                FROM gold.mart_daily_revenue
                WHERE DATE_TRUNC('month', date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
            )
            SELECT
                cm.revenue_mtd,
                cm.orders_mtd,
                cm.aov,
                CASE
                    WHEN pm.revenue_prev = 0 THEN NULL
                    ELSE ROUND(
                        ((cm.revenue_mtd - pm.revenue_prev) / pm.revenue_prev * 100)::numeric, 2
                    )
                END AS revenue_growth_pct
            FROM current_month cm, prev_month pm
        """))
        row = result.mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="No revenue data found")
        return {
            "total_revenue_mtd": float(row["revenue_mtd"]),
            "order_count_mtd":   int(row["orders_mtd"]),
            "aov":               float(row["aov"]),
            "revenue_growth_pct": float(row["revenue_growth_pct"]) if row["revenue_growth_pct"] else None,
            "last_updated":      datetime.utcnow(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")

@router.get("/by-category", response_model=List[CategoryPerformance])
def get_category_performance(
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db)
):
    """Revenue breakdown by product category."""
    try:
        result = db.execute(text("""
            SELECT
                date,
                category,
                revenue,
                units_sold,
                order_count,
                revenue_share_pct
            FROM gold.mart_category_performance
            WHERE date >= CURRENT_DATE - INTERVAL ':days days'
            ORDER BY date DESC, revenue DESC
        """), {"days": days})
        rows = result.mappings().all()
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")
