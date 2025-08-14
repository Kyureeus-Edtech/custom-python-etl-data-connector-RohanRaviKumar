# AbuseIPDB ETL Connector

## Endpoint Used
`GET https://api.abuseipdb.com/api/v2/check?ipAddress=IP&maxAgeInDays=90`

## API Documentation
[https://docs.abuseipdb.com/#check-endpoint](https://docs.abuseipdb.com/#check-endpoint)

## Setup Instructions
1. Clone the repository and switch to your branch.
2. Create `.env` file in the root directory:
    API_KEY=4afcd48cf220bdc4cfdbc2782b8be23e2011e89d4e83a002c3a490e47f5a12f34232bb9a314703dc
3. Install dependencies:
    pip install -r requirements.txt
4. Start MongoDB locally or update the connection string in `etl_connector.py`.
5. Run the connector:
    python etl_connector.py