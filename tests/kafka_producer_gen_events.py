import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define user, product, and campaign data
users = [f"user_{i}" for i in range(1, 101)]
products = [f"product_{i}" for i in range(1, 51)]
campaigns = [f"campaign_{i}" for i in range(1, 11)]
page_urls = [f"/page_{i}" for i in range(1, 21)]
event_types = ["click", "view"]
interaction_types = ["view", "add_to_cart", "purchase"]
signup_sources = ["organic", "ad_campaign", "referral"]

def generate_event():
    event_type = random.choice([
        "user_click",
        "product_interaction",
        "conversion_event",
        "user_signup",
        "session_data"
    ])

    if event_type == "user_click":
        return {
            "event_id": str(random.randint(100000, 999999)),
            "user_id": random.choice(users),
            "page_url": random.choice(page_urls),
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": random.choice(event_types),
            "referrer": random.choice(["google", "facebook", "twitter", ""])
        }
    
    elif event_type == "product_interaction":
        return {
            "interaction_id": str(random.randint(100000, 999999)),
            "user_id": random.choice(users),
            "product_id": random.choice(products),
            "interaction_type": random.choice(interaction_types),
            "timestamp": datetime.utcnow().isoformat(),
            "session_id": str(random.randint(10000, 99999))
        }
    
    elif event_type == "conversion_event":
        return {
            "conversion_id": str(random.randint(100000, 999999)),
            "user_id": random.choice(users),
            "campaign_id": random.choice(campaigns),
            "product_id": random.choice(products),
            "conversion_value": round(random.uniform(10.0, 500.0), 2),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    elif event_type == "user_signup":
        return {
            "signup_id": str(random.randint(100000, 999999)),
            "user_id": random.choice(users),
            "email": f"user{random.randint(1, 1000)}@example.com",
            "timestamp": datetime.utcnow().isoformat(),
            "signup_source": random.choice(signup_sources)
        }
    
    elif event_type == "session_data":
        start_time = datetime.utcnow()
        end_time = start_time + random.choice([timedelta(minutes=5), timedelta(minutes=10), timedelta(minutes=15)])
        return {
            "session_id": str(random.randint(10000, 99999)),
            "user_id": random.choice(users),
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration": (end_time - start_time).total_seconds(),
            "pages_viewed": random.randint(1, 10),
            "actions": [random.choice(event_types) for _ in range(random.randint(1, 5))]
        }

def send_event():
    event = generate_event()
    producer.send('website_events', event)
    producer.flush()
    print(f"Sent event: {event}")

if __name__ == "__main__":
    while True:
        send_event()
        time.sleep(random.uniform(0.1, 1.0))  # Adjust the rate of event generation as needed
