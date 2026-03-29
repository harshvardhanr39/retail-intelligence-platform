import uuid
import json
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

load_dotenv()

#-- Kafka configuration
BOOTSRTAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "retail.events.clickstream"

producer = Producer({
    "bootstrap.servers": BOOTSRTAP_SERVERS,
    "client.id": "retail.events-generator",
})

print(f"✅ Connected to Kafka at {BOOTSRTAP_SERVERS}")
print(f"✅ Producing to topic: {TOPIC}")

#-- Static data for event generation
PRODUCT_IDS = [str(i) for i in range(1, 21)]  # 20 products

PAGES = [
    "/", "/products", "/products/electronics",
    "/products/jewelery", "/products/mens-clothing",
    "/products/womens-clothing", "/sale", "/about",
]

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.35, 0.55, 0.10]

SOURCES = ["organic", "paid_search", "email", "social", "referral", "direct"]
SOURCE_WEIGHTS = [0.35, 0.20, 0.20, 0.15, 0.05, 0.05]

#-- funnel probabilities
FUNNEL = {
    "session_start": 1.0,
    "page_view": 0.9,
    "product_view": 0.7,
    "add_to_cart": 0.4,
    "checkout_start": 0.6,
    "purchase_complete": 0.75
}

#-- delivery report callback
def delivery_report(err, msg):
    if err:
        print(f"❌ Message delivery failed: {err}")

#-- build one event
def make_event(event_type, session_id, user_id, device, source, ts, product_id=None):
    page_url = f"products/{product_id}" if product_id else random.choice(PAGES)

    properties = {
        "device_type": device,
        "traffic_source": source,
        "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
        "os": random.choice(["Windows", "macOS", "iOS", "Android"]),
    }

    if event_type == "page_view":
        properties["page_title"] = page_url.replace("/", " ").strip().title() or "Home"

        properties["time_on_page_seconds"] = random.randint(5, 300)  # seconds

    elif event_type == "product_view":
        properties["product_id"] = product_id
        properties["time_on_page_seconds"] = random.randint(10, 180)  # seconds
    
    elif event_type == "add_to_cart":
        properties["product_id"] = product_id
        properties["quantity"] = random.randint(1, 3)
    
    elif event_type == "checkout_start":
        properties["cart_value"] = round(random.uniform(10, 500), 2)
        properties["num_items"] = random.randint(1, 5)
    
    elif event_type == "purchase_complete":
        properties["order_id"] = str(uuid.uuid4())
        properties["order_value"] = round(random.uniform(10, 500), 2)
        properties["payment_method"] = random.choice(["credit_card", "paypal", "apple_pay"])



    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "user_id": str(random.randint(1, 500)),
        "event_type": event_type,
        "product_id": product_id,
        "page_url":   page_url,
        "timestamp":  ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "properties": properties,
    }
    

#-- simulate 1000 sessions
Num_sessions = 1000
total_events = 0
now = datetime.utcnow()

print(f"🚀 Generating clickstream events for {Num_sessions} sessions...")

for s in range(1, Num_sessions + 1):
    session_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    device = random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0]
    source = random.choices(SOURCES, weights=SOURCE_WEIGHTS)[0]
    product_id = random.choice(PRODUCT_IDS)

    #-- session start
    session_start = now - timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))

    #each event in the session is a few seconds apart
    ts = session_start
    session_events = []

    #--walk through the funnel
    #1.session_start - always happens
    session_events.append(make_event("session_start", session_id, user_id, device, source, ts))
    ts += timedelta(seconds=random.randint(1, 5))

    #2.page_view - 90% chance
    if random.random() < FUNNEL["page_view"]:
        session_events.append(make_event("page_view", session_id, user_id, device, source, ts))
        ts += timedelta(seconds=random.randint(10, 60))
    else:
        # Bounced — no more events
        pass

    #3.product_view - 70% chance
    if len(session_events)>= 2 and random.random() < FUNNEL["product_view"]:
        session_events.append(make_event("product_view", session_id, user_id, device, source, ts, product_id))
        ts += timedelta(seconds=random.randint(15, 120))

    #4.add_to_cart - 40% chance
    if len(session_events)>= 3 and random.random() < FUNNEL["add_to_cart"]:
        session_events.append(make_event("add_to_cart", session_id, user_id, device, source, ts, product_id))
        ts += timedelta(seconds=random.randint(5, 30))
    
    #5.checkout_start - 60% chance
    if len(session_events)>= 4 and random.random() < FUNNEL["checkout_start"]:
        session_events.append(make_event("checkout_start", session_id, user_id, device, source, ts))
        ts += timedelta(seconds=random.randint(30, 180))
    
    #6.purchase_complete - 75% chance
    if len(session_events)>= 5 and random.random() < FUNNEL["purchase_complete"]:
        session_events.append(make_event("purchase_complete", session_id, user_id, device, source, ts, product_id)) 
    

    #-- produce events to Kafka
    for event in session_events:
        producer.produce(
            topic = TOPIC, 
            key = session_id,
            value = json.dumps(event), 
            callback=delivery_report
        )
        total_events += 1

    #poll to handle delivery callbacks
    producer.poll(0)

    #progrees log every 100 sessions
    if s % 100 == 0:
        print(f"  ✅ {s:,}/{Num_sessions:,} sessions — {total_events:,} events so far")

# ── Flush all remaining messages ─────────────────────────
print("\n⏳ Flushing remaining messages to Kafka...")
producer.flush()
 
print(f"\n🎉 Done!")
print(f"   Sessions  : {Num_sessions:,}")
print(f"   Events    : {total_events:,}")
print(f"   Topic     : {TOPIC}")
print(f"\nVerify in Kafka UI: http://localhost:8085")
print(f"Or run: docker exec retail-kafka kafka-console-consumer \\")
print(f"  --bootstrap-server localhost:9092 \\")
print(f"  --topic {TOPIC} \\")
print(f"  --from-beginning --max-messages 5")