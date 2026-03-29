import pandas as pd 
import random
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import psycopg2

load_dotenv()

today = datetime.utcnow().date()
output_dir = 'data/landing/inventory'
os.makedirs(output_dir, exist_ok=True)

#-- fetch product ids from database
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()
cur.execute("SELECT product_id, title FROM source_data.products;")
products = cur.fetchall()
cur.close()
conn.close()
 
if not products:
    print("❌ No products found. Run generate_orders.py first.")
    exit(1)
 
print(f"✅ Fetched {len(products)} products from database")

#-- generate 500 SKU rows
warehouses = ['WH-01', 'WH-02', 'WH-03', 'WH-04', 'WH-05']
supplier_codes = ['SUP-A1', 'SUP-B2', 'SUP-C3', 'SUP-D4', 'SUP-E5', 'SUP-F6']

rows = []

for i in range(500):
    product_id, product_name = random.choice(products)
    warehouse_id = random.choice(warehouses)
    sku_id = f"SKU-{product_id}-{warehouse_id}"

    quantity_on_hand = random.randint(0, 1000)
    reorder_point = random.randint(10, 150)

    #last_count_date =random date in last 30 days
    days_ago = random.randint(0, 30)
    last_count_date = today - timedelta(days=days_ago)

    supplier_code = random.choice(supplier_codes)
        
    rows.append({
        'sku_id': sku_id,
        'product_name': product_name,
        'product_id': product_id,
        'warehouse_id': warehouse_id,
        'quantity_on_hand': quantity_on_hand,
        'reorder_point': reorder_point,
        'last_count_date': last_count_date,
        'supplier_code': supplier_code
    })

df = pd.DataFrame(rows) 

#-- Intentional issue 1: 5% negative quantity_on_hand
num_negative = int(0.05 * len(df))
negative_indices = random.sample(range(len(df)), num_negative)
df.loc[negative_indices, 'quantity_on_hand'] = -df.loc[negative_indices, 'quantity_on_hand'].abs()
print(f"✅ Intentional issue introduced: {num_negative} rows with negative quantity_on_hand (5%)")

#-- Intentional issue 2: 2% null dates
num_null_dates = int(0.02 * len(df))
null_date_indices = random.sample(range(len(df)), num_null_dates)
df.loc[null_date_indices, 'last_count_date'] = pd.NaT
print(f"✅ Intentional issue introduced: {num_null_dates} rows with null last_count_date (2%)")

#-- Intentional issue 3: 3% duplicate rows
num_dupes = int(len(df) * 0.03)
dupe_rows = df.sample(n=num_dupes, replace=True).copy()
# Slightly change quantity to make them realistic duplicates
dupe_rows["quantity_on_hand"] = dupe_rows["quantity_on_hand"].apply(
    lambda x: x + random.randint(-5, 5) if pd.notna(x) else x
)
df = pd.concat([df, dupe_rows], ignore_index=True)
print(f"   Introduced {num_dupes} duplicate rows (3%)")

#-- Save to CSV
output_path = os.path.join(output_dir, f"inventory_{today}.csv")
df.to_csv(output_path, index=False)
print(f"\n✅ Inventory data generated and saved to {output_path}")
print("\n🎉 Done!")
print(f"   Output file : {output_path}")
print(f"   Total rows  : {len(df)} (500 base + {num_dupes} duplicates)")
print(f"   Negatives   : {num_negative} rows")
print(f"   Null dates  : {num_null_dates} rows")
print(f"   Duplicates  : {num_dupes} rows")
print("\nPreview:")
print(df.head(5).to_string())