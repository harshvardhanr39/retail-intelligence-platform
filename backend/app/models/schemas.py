from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime

class DailyRevenue(BaseModel):
    date: date
    total_revenue: float
    order_count: int
    avg_order_value: float
    revenue_7day_avg: Optional[float] = None
    prev_day_revenue: Optional[float] = None
    revenue_growth_pct: Optional[float] = None

    class Config:
        from_attributes = True

class RevenueSummary(BaseModel):
    total_revenue_mtd: float
    order_count_mtd: int
    aov: float
    revenue_growth_pct: Optional[float] = None
    last_updated: datetime

class CategoryPerformance(BaseModel):
    date: date
    category: str
    revenue: float
    units_sold: int
    order_count: int
    revenue_share_pct: float

    class Config:
        from_attributes = True

class InventoryItem(BaseModel):
    sku_id: str
    product_name: Optional[str] = None
    product_id: Optional[str] = None
    warehouse_id: Optional[str] = None
    quantity_on_hand: int
    reorder_point: Optional[int] = None
    days_of_stock: Optional[float] = None
    status_flag: str
    avg_daily_units_sold: Optional[float] = None

    class Config:
        from_attributes = True

class FunnelMetric(BaseModel):
    date: date
    sessions: int
    page_views: Optional[int] = None
    product_views: Optional[int] = None
    add_to_carts: Optional[int] = None
    checkouts: Optional[int] = None
    purchases: Optional[int] = None
    cvr_pct: float
    cart_rate_pct: Optional[float] = None

    class Config:
        from_attributes = True

class TopProduct(BaseModel):
    rank: int
    product_id: str
    product_name: Optional[str] = None
    category: Optional[str] = None
    total_revenue: float
    units_sold: int
    order_count: int
    avg_selling_price: Optional[float] = None

    class Config:
        from_attributes = True

class PipelineRun(BaseModel):
    run_id: str
    source_name: str
    status: str
    rows_extracted: Optional[int] = None
    rows_written: Optional[int] = None
    started_at: datetime
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True

class DataFreshness(BaseModel):
    source_name: str
    last_run_at: Optional[datetime] = None
    last_watermark: Optional[datetime] = None
    hours_since_update: Optional[float] = None
    status: str   # green / amber / red
