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


# ── 4. Generate and insert 500 customers ─────────────────
print("\n📦 Generating 500 customers...")

segments = ["consumer", "corporate", "home_office"]
segment_weights = [0.6, 0.3, 0.1]

customers = []
customer_ids = []

for _ in range(500):
    cid = str(uuid.uuid4())
    customer_ids.append(cid)
    customers.append((
        cid,
        fake.unique.email(),
        fake.first_name(),
        fake.last_name(),
        fake.city(),
        fake.country(),
        random.choices(segments, weights=segment_weights)[0],
    ))

execute_batch(cur, """
    INSERT INTO source_data.customers
        (customer_id, email, first_name, last_name, city, country, segment)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
""", customers)

conn.commit()
print(f"  ✅ Inserted {len(customers)} customers")


# ── 5. Fetch products from FakeStoreAPI ───────────────────
print("\n📦 Fetching products from FakeStoreAPI...")

raw_products = []
try:
    resp = requests.get("https://fakestoreapi.com/products", timeout=10)
    resp.raise_for_status()
    raw_products = resp.json()
    print(f"  ✅ Got {len(raw_products)} products from FakeStoreAPI")
except Exception as e:
    print(f"  ⚠️  FakeStoreAPI failed ({e}), trying fallback...")
    try:
        resp = requests.get("https://dummyjson.com/products?limit=100", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        # Normalise dummyjson shape to match fakestoreapi
        raw_products = [
            {
                "id": p["id"],
                "title": p["title"],
                "category": p.get("category", "general"),
                "price": p["price"],
                "description": p.get("description", ""),
                "image": p.get("thumbnail", ""),
                "rating": {"rate": p.get("rating", 0), "count": p.get("stock", 0)},
            }
            for p in data.get("products", [])
        ]
        print(f"  ✅ Got {len(raw_products)} products from fallback API")
    except Exception as e2:
        print(f"  ❌ Both APIs failed: {e2}. Using mock products.")
        raw_products = [
            {"id": i, "title": f"Product {i}", "category": "general",
             "price": round(random.uniform(5, 500), 2), "description": "",
             "image": "", "rating": {"rate": 4.0, "count": 100}}
            for i in range(1, 21)
        ]

# Build a lookup dict: product_id → price (for order item generation)
product_rows = []
product_catalog = {}  # {product_id_str: price}

for p in raw_products:
    pid = str(p["id"])
    price = float(p["price"])
    product_catalog[pid] = price
    product_rows.append((
        pid,
        p["title"][:500],
        p.get("category", "general")[:100],
        price,
        p.get("description", "")[:2000],
        p.get("image", "")[:500],
        float(p["rating"]["rate"]) if p["rating"]["rate"] else 0.0,
        int(p["rating"]["count"]) if p["rating"]["count"] else 0,
    ))

execute_batch(cur, """
    INSERT INTO source_data.products
        (product_id, title, category, price, description, image_url, rating, rating_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
""", product_rows)

conn.commit()
print(f"  ✅ Inserted {len(product_rows)} products")

product_ids = list(product_catalog.keys())


# ── 6. Generate 10,000 orders ────────────────────────────
print("\n📦 Generating 10,000 orders...")

statuses = ["delivered", "shipped", "processing", "cancelled", "pending"]
status_weights = [0.60, 0.20, 0.10, 0.05, 0.05]

now = datetime.utcnow()

order_batch = []
item_batch = []

for i in range(1, 10_001):
    order_id = str(uuid.uuid4())
    customer_id = random.choice(customer_ids)
    status = random.choices(statuses, weights=status_weights)[0]

    # Random date within the last 90 days
    days_ago = random.randint(0, 90)
    hours_ago = random.randint(0, 23)
    created_at = now - timedelta(days=days_ago, hours=hours_ago)

    # updated_at is slightly after created_at
    updated_at = created_at + timedelta(hours=random.randint(1, 48))
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

    # ── 8. Batch insert every 1,000 orders ───────────────
    if i % 1000 == 0:
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
        # ── 10. Progress every 1,000 records ─────────────
        print(f"  ✅ Inserted {i:,}/10,000 orders ({len(item_batch)} items in this batch)")

        order_batch = []
        item_batch = []

# Insert any remaining rows (in case total isn't divisible by 1000)
if order_batch:
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
print(f"   Customers : 500")
print(f"   Products  : {len(product_rows)}")
print(f"   Orders    : 10,000")
print(f"   Order items: ~{10_000 * 3:,} (avg 3 per order)")