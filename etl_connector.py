import os
import argparse
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timezone

# Load variables from ENV_TEMPLATE instead of .env
load_dotenv(dotenv_path="ENV_TEMPLATE")


API_KEY = os.getenv("API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME", "etl_database")  # default if not set

if not API_KEY:
    raise ValueError("API_KEY not found in environment variables.")

# Build MongoDB connection string
if DB_USER and DB_PASS:
    mongo_uri = f"mongodb://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?authSource=admin"
else:
    mongo_uri = f"mongodb://{DB_HOST}/{DB_NAME}"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[DB_NAME]
collection = db["abuseipdb_raw"]

def extract(ip_address: str, max_age_days: int = 90):
    """Fetch data from AbuseIPDB API for a given IP address."""
    url = "https://api.abuseipdb.com/api/v2/check"
    headers = {
        "Key": API_KEY,
        "Accept": "application/json"
    }
    params = {
        "ipAddress": ip_address,
        "maxAgeInDays": max_age_days
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"API Request Failed: {response.status_code} {response.text}")
    return response.json()

def transform(data: dict):
    """Prepare the data for MongoDB insertion."""
    return {
        "ipAddress": data.get("data", {}).get("ipAddress"),
        "isPublic": data.get("data", {}).get("isPublic"),
        "abuseConfidenceScore": data.get("data", {}).get("abuseConfidenceScore"),
        "countryCode": data.get("data", {}).get("countryCode"),
        "usageType": data.get("data", {}).get("usageType"),
        "isp": data.get("data", {}).get("isp"),
        "domain": data.get("data", {}).get("domain"),
        "hostnames": data.get("data", {}).get("hostnames"),
        "totalReports": data.get("data", {}).get("totalReports"),
        "lastReportedAt": data.get("data", {}).get("lastReportedAt"),
        "fetchedAt": datetime.now(timezone.utc)

    }

def load(record: dict):
    """Insert the record into MongoDB."""
    collection.insert_one(record)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AbuseIPDB ETL Connector")
    parser.add_argument("--ip", type=str, required=True, help="IP address to check")
    parser.add_argument("--days", type=int, default=int(os.getenv("MAX_AGE_DAYS", 90)),
                        help="Max age of reports in days")
    args = parser.parse_args()

    print(f"Fetching data for IP: {args.ip}")
    raw_data = extract(args.ip, args.days)
    transformed_data = transform(raw_data)
    load(transformed_data)
    print("Data successfully loaded into MongoDB.")
