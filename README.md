# SRP Energy Usage Collector

A Python script that automatically fetches and stores daily energy usage data from Salt River Project (SRP) utility accounts. This tool interfaces with SRP's API to collect detailed hourly electricity consumption and cost data, storing it in a MySQL database for further analysis.

## Features
- Automated daily energy usage data collection
- Stores detailed metrics including:
  - Date and hour of usage
  - Energy consumption (kWh)
  - Associated costs
  - ISO formatted timestamps
- Configurable for historical data backfilling
- Secure credential management using environment variables
- Error handling and logging

## Prerequisites
- Python 3.x
- MySQL Database
- SRP Energy account credentials
- Required Python packages:
  - srpenergy
  - MySQLdb
  - python-dotenv

## Environment Variables
```
BI_HOST=your_mysql_host
BI_USER=your_mysql_user
BI_PASS=your_mysql_password
GEN_DB_NAME=your_database_name
SRP_ACCOUNT=your_srp_account_number
SRP_USER=your_srp_username
SRP_PASS=your_srp_password
```

## Usage
Run the script daily to collect the previous day's energy usage data:
```bash
python srp-daily.py
```

For historical data collection, adjust the loop parameters in the script as noted in the comments.
