import json
import uuid
import random
import os
from datetime import datetime

# ==========================================
# DEMO DATA GENERATION CONFIG
# ==========================================
# Current batch date
TARGET_DT = "2026-04-01"

# Target IDs for SCD Type 2 demonstration
# (These will appear as updates if already in Silver)
UPDATE_BUSINESS_ID = "85361242-02a6-49f1-ae8" 
UPDATE_USER_ID = "8eacab81-2e6f-45dc-a23"

def generate_business_record(business_id=None, add_extra=False):
    bid = business_id or str(uuid.uuid4())[:22]
    # GENERIC NAMES (no mention of interview)
    record = {
        "business_id": bid,
        "name": f"Regular Business {bid[:8]}",
        "address": f"{random.randint(100, 999)} Commerce St",
        "city": "Austin",
        "state": "TX",
        "postal_code": str(random.randint(70000, 78999)),
        "latitude": round(random.uniform(30.2, 30.3), 6),
        "longitude": round(random.uniform(-97.8, -97.7), 6),
        "stars": round(random.uniform(3, 5) * 2) / 2,
        "review_count": random.randint(10, 1000),
        "is_open": 1,
        "attributes": {"ByAppointmentOnly": "False", "BusinessParking": '{"garage": true}'},
        "hours": {"Monday": "9:0-18:0", "Tuesday": "9:0-18:0"},
        "categories": "Professional Services, Business",
    }
    if add_extra:
        record["data_quality_tag"] = "batch-v2-verified"
    return record

def generate_user_record(user_id=None, add_extra=False):
    uid = user_id or str(uuid.uuid4())[:22]
    # GENERIC NAMES
    record = {
        "user_id": uid,
        "name": f"Member {uid[:8]}",
        "review_count": random.randint(1, 100),
        "yelping_since": f"2023-01-01 00:00:00",
        "useful": random.randint(0, 50),
        "funny": random.randint(0, 10),
        "cool": random.randint(0, 50),
        "elite": "",
        "friends": ",".join([str(uuid.uuid4())[:22] for _ in range(5)]),
        "fans": random.randint(0, 10),
        "average_stars": round(random.uniform(3.5, 5.0), 2),
        "compliment_hot": 0,
        "compliment_more": 0,
        "compliment_profile": 0,
        "compliment_cute": 0,
        "compliment_list": 0,
        "compliment_note": 0,
        "compliment_plain": 0,
        "compliment_cool": 0,
        "compliment_funny": 0,
        "compliment_writer": 0,
        "compliment_photos": 0,
    }
    if add_extra:
        record["user_loyalty_tier"] = "verified"
    return record

def generate_review_record():
    return {
        "review_id": str(uuid.uuid4())[:22],
        "user_id": str(uuid.uuid4())[:22],
        "business_id": str(uuid.uuid4())[:22],
        "stars": float(random.randint(4, 5)),
        "useful": random.randint(0, 10),
        "funny": random.randint(0, 5),
        "cool": random.randint(0, 5),
        "text": "Great service and very professional experience. Highly recommend for any corporate needs.",
        "date": datetime.now().isoformat()
    }

def main():
    entities = ["business", "user", "review", "checkin", "tip"]
    # Lightweight volume for instant demo (~5-10MB total)
    counts = {
        "business": 500,
        "user": 500,
        "review": 1000,
        "checkin": 500,
        "tip": 500
    }

    print(f"🔄 Preparing Clean Production Batch for {TARGET_DT}...")

    for entity in entities:
        path = f"data/yelp/raw/entity={entity}/dt={TARGET_DT}"
        os.makedirs(path, exist_ok=True)
        file_path = f"{path}/{entity}_batch.json"
        
        with open(file_path, "w") as f:
            if entity == "business":
                # SCD2 UPDATE: Record 1
                f.write(json.dumps(generate_business_record(UPDATE_BUSINESS_ID)) + "\n")
                # SCHEMA EVOLUTION: Record 2
                f.write(json.dumps(generate_business_record(add_extra=True)) + "\n")
            elif entity == "user":
                f.write(json.dumps(generate_user_record(UPDATE_USER_ID)) + "\n")
                f.write(json.dumps(generate_user_record(add_extra=True)) + "\n")
            
            # Data volume
            for i in range(counts[entity]):
                if entity == "business": record = generate_business_record()
                elif entity == "user": record = generate_user_record()
                elif entity == "review": record = generate_review_record()
                elif entity == "checkin": record = {"business_id": str(uuid.uuid4())[:22], "date": "2024-01-01"}
                elif entity == "tip": record = {"user_id": str(uuid.uuid4())[:22], "business_id": str(uuid.uuid4())[:22], "text": "Good place", "date": "2024-01-01", "compliment_count": random.randint(0, 5)}
                f.write(json.dumps(record) + "\n")
        
        size = os.path.getsize(file_path) / (1024 * 1024)
        print(f"  ✅ {entity} batch ready ({size:.2f} MB)")

if __name__ == "__main__":
    main()
