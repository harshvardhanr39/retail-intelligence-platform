from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from app.db.connection import get_db
from app.models.schemas import TopProduct

router = APIRouter(prefix="/products", tags=["Products"])

@router.get("/top", response_model=List[TopProduct])
def get_top_products(
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """Top products ranked by all-time revenue."""
    try:
        result = db.execute(text("""
            SELECT
                rank,
                product_id,
                product_name,
                category,
                total_revenue,
                units_sold,
                order_count,
                avg_selling_price
            FROM gold.mart_top_products
            ORDER BY rank ASC
            LIMIT :limit
        """), {"limit": limit})
        rows = result.mappings().all()
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")
