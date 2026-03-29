from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from app.db.connection import get_db
from app.models.schemas import InventoryItem

router = APIRouter(prefix="/inventory", tags=["Inventory"])

@router.get("/health", response_model=List[InventoryItem])
def get_inventory_health(
    status: str = Query(default=None, description="Filter by status: out_of_stock, low_stock, healthy, overstock"),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db)
):
    """Inventory health status for all SKUs."""
    try:
        where = "WHERE status_flag = :status" if status else ""
        result = db.execute(text(f"""
            SELECT
                sku_id,
                product_name,
                product_id,
                warehouse_id,
                quantity_on_hand,
                reorder_point,
                days_of_stock,
                status_flag,
                avg_daily_units_sold
            FROM gold.mart_inventory_health
            {where}
            ORDER BY days_of_stock ASC NULLS FIRST
            LIMIT :limit
        """), {"status": status, "limit": limit})
        rows = result.mappings().all()
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")

@router.get("/alerts", response_model=List[InventoryItem])
def get_inventory_alerts(db: Session = Depends(get_db)):
    """Only items that need attention — out of stock or low stock."""
    try:
        result = db.execute(text("""
            SELECT
                sku_id, product_name, product_id, warehouse_id,
                quantity_on_hand, reorder_point, days_of_stock,
                status_flag, avg_daily_units_sold
            FROM gold.mart_inventory_health
            WHERE status_flag IN ('out_of_stock', 'low_stock')
            ORDER BY
                CASE status_flag
                    WHEN 'out_of_stock' THEN 1
                    WHEN 'low_stock'    THEN 2
                END,
                days_of_stock ASC NULLS FIRST
        """))
        rows = result.mappings().all()
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")
