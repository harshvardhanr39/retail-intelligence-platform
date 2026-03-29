import uuid
import random
import requests
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

# ── 1. Load environment variables ─────────────────────────
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
print(DATABASE_URL)

# ── 2. Create Faker instance ──────────────────────────────
fake = Faker("en_US")

# ── 3. Connect to Postgres ────────────────────────────────
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()
print("✅ Connected to database")

# ── 4. Generate 50-100 orders ────────────────────────────
num_orders = random.randint(50, 100)
print(f"\n📦 Generating {num_orders} orders...")

statuses = ["processing","pending"]
status_weights = [0.40, 0.60]

now = datetime.utcnow()

order_batch = []
item_batch = []

cur.execute("select customer_id from source_data.customers;")
customer_ids = [row[0] for row in cur.fetchall()]

cur.execute("select product_id from source_data.products;")
product_ids = [row[0] for row in cur.fetchall()]
cur.execute("select product_id, price from source_data.products;")
product_catalog = {str(row[0]): float(row[1]) for row in cur.fetchall()}

for i in range(1, num_orders + 1):
    order_id = str(uuid.uuid4())
    customer_id = random.choice(customer_ids)
    status = random.choices(statuses, weights=status_weights)[0]

    created_at = now

    # updated_at is slightly after created_at
    updated_at = created_at + timedelta(hours=random.randint(1, 6))
    if updated_at > now:
        updated_at = now

    # ── 7. Generate 1–5 order items ──────────────────────
    num_items = random.randint(1, 5)
    chosen_products = random.choices(product_ids, k=num_items)

    subtotal = 0.0
    items_for_order = []

    for product_id in chosen_products:
        base_price = product_catalog[product_id]
        # unit_price = base price ± 5% variation
        variation = random.uniform(-0.05, 0.05)
        unit_price = round(base_price * (1 + variation), 2)
        quantity = random.randint(1, 5)
        line_total = round(unit_price * quantity, 2)
        subtotal += line_total

        items_for_order.append((
            str(uuid.uuid4()),  # item_id
            order_id,
            product_id,
            quantity,
            unit_price,
        ))

    subtotal = round(subtotal, 2)
    discount = round(subtotal * random.uniform(0, 0.1), 2)   # 0–10% discount
    tax = round((subtotal - discount) * 0.08, 2)              # 8% tax
    shipping = round(random.uniform(0, 15), 2)
    total_amount = round(subtotal - discount + tax + shipping, 2)

    order_batch.append((
        order_id,
        customer_id,
        status,
        "USD",
        subtotal,
        discount,
        tax,
        shipping,
        total_amount,
        created_at,
        updated_at,
    ))

    item_batch.extend(items_for_order)

    execute_batch(cur, """
        INSERT INTO source_data.orders
            (order_id, customer_id, status, currency,
                subtotal, discount_amount, tax_amount, shipping_amount,
                total_amount, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, order_batch)

    execute_batch(cur, """
        INSERT INTO source_data.order_items
            (item_id, order_id, product_id, quantity, unit_price)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, item_batch)

    conn.commit()

cur.close()
conn.close()

print("\n🎉 Done!")
print(f"   Orders    : {num_orders}")
print(f"   Order items: ~{len(item_batch):,} (avg {len(item_batch)/num_orders:.1f} per order)")