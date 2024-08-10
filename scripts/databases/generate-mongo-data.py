import pymongo
from pymongo import MongoClient
import random
import string
import sys

# MongoDB Atlas connection URI
mongo_uri = "your_uri"
database_name = "your_db_name"  # Replace with your actual database name

# Function to generate more realistic customer profiles data
def generate_customer_profiles(num_records):
    data = []
    for i in range(num_records):
        profile = {
            "customer_id": i + 1,
            "name": ''.join(random.choices(string.ascii_uppercase, k=random.randint(5, 10))),
            "age": random.randint(18, 65),
            "gender": random.choice(["Male", "Female"]),
            "email": f"user{i + 1}@example.com",
            "address": {
                "street": ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(10, 20))),
                "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
                "state": random.choice(["NY", "CA", "IL", "TX", "AZ"]),
                "zipcode": ''.join(random.choices(string.digits, k=5))
            }
        }
        data.append(profile)
    return data

# Function to generate more realistic product catalogs data
def generate_product_catalogs(num_records):
    data = []
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Sports & Outdoors", "Books"]
    for i in range(num_records):
        product = {
            "product_id": i + 1,
            "name": f"Product {i + 1}",
            "category": random.choice(categories),
            "price": round(random.uniform(10, 1000), 2),
            "description": ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(20, 50)))
        }
        data.append(product)
    return data

# Function to generate more realistic campaign metadata
def generate_campaign_metadata(num_records):
    data = []
    channels = ["Email", "Social Media", "Print Media", "Online Ads", "Events"]
    for i in range(num_records):
        campaign = {
            "campaign_id": i + 1,
            "name": f"Campaign {i + 1}",
            "start_date": f"2024-07-{random.randint(1, 15)}",
            "end_date": f"2024-07-{random.randint(16, 31)}",
            "channel": random.choice(channels),
            "budget": round(random.uniform(1000, 50000), 2)
        }
        data.append(campaign)
    return data

# Function to insert data into MongoDB
def insert_data(collection_name, data):
    try:
        client = MongoClient(mongo_uri)
        db = client[database_name]  # Accessing the specified database
        collection = db[collection_name]
        collection.insert_many(data)
        print(f"Inserted {len(data)} records into '{collection_name}' collection.")
    except pymongo.errors.PyMongoError as e:
        print(f"Error inserting data into '{collection_name}' collection:", e)
        sys.exit(1)
    finally:
        client.close()

if __name__ == "__main__":
    try:
        # Customer Profiles: 100MB
        customer_data = generate_customer_profiles(10000)  # Generate 10,000 customer profiles
        insert_data("customer_profiles", customer_data)

        # Product Catalogs: 100MB
        product_data = generate_product_catalogs(5000)  # Generate 5,000 products in catalog
        insert_data("product_catalogs", product_data)

        # Campaign Metadata: 100MB
        campaign_data = generate_campaign_metadata(2000)  # Generate 2,000 campaign metadata entries
        insert_data("campaign_metadata", campaign_data)

    except Exception as e:
        print("An error occurred during data generation and insertion:", e)
