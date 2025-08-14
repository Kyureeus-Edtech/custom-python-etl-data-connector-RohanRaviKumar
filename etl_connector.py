import os
import argparse
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

# Load environment variables
load_dotenv()
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise ValueError("API_KEY not found in environment variables.")

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["etl_database"]
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
        "fetchedAt": datetime.utcnow()
    }

def load(record: dict):
    """Insert the record into MongoDB."""
    collection.insert_one(record)

if __name__ == "__main__":
    # Command-line arguments
    parser = argparse.ArgumentParser(description="AbuseIPDB ETL Connector")
    parser.add_argument("--ip", type=str, required=True, help="IP address to check")
    parser.add_argument("--days", type=int, default=90, help="Max age of reports in days")
    args = parser.parse_args()

    print(f"Fetching data for IP: {args.ip}")
    raw_data = extract(args.ip, args.days)
    transformed_data = transform(raw_data)
    load(transformed_data)
    print("Data successfully loaded into MongoDB.")