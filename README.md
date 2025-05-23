# SRP Energy Usage Collector

A Python script suite that automatically fetches and stores daily energy usage data from Salt River Project (SRP) utility accounts, enhanced with weather data correlation. This tool interfaces with SRP's API to collect detailed hourly electricity consumption and cost data, then enriches it with corresponding weather information for comprehensive energy analysis.

## Features
- Automated daily energy usage data collection from SRP
- Weather data integration using Open-Meteo API
- Stores detailed metrics including:
  - Date and hour of usage
  - Energy consumption (kWh)
  - Associated costs
  - ISO formatted timestamps
  - Temperature and humidity data
- Configurable for historical data backfilling
- Secure credential management using environment variables
- Error handling and logging
- Automatic weather data backfilling for existing records

## Prerequisites
- Python 3.x
- MySQL Database
- SRP Energy account credentials
- Required Python packages:
  - srpenergy
  - MySQLdb
  - python-dotenv
  - mysql-connector-python
  - openmeteo-requests
  - pandas
  - requests-cache
  - retry-requests
  - numpy

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

## Database Schema
The script expects a table named `srp` with the following columns:
- `id` (primary key)
- `date`
- `hour`
- `isotime`
- `kwh`
- `cost`
- `datetime`
- `temperature`
- `humidity`

## Usage

### Daily Data Collection Workflow
Run these scripts in sequence for complete daily data collection:

1. **Collect Energy Usage Data:**
   ```bash
   python srp-daily.py
   ```

2. **Enrich with Weather Data:**
   ```bash
   python weather.py
   ```

### Historical Data Collection
For historical data collection, adjust the loop parameters in `srp-daily.py` as noted in the comments, then run `weather.py` to backfill weather data for the newly added records.

## Script Details

### srp-daily.py
Fetches daily energy usage data from SRP and stores it in the MySQL database. Configured to collect the previous day's data by default.

### weather.py
Enriches existing energy data with weather information from Open-Meteo API. The script:
- Identifies records missing temperature or humidity data
- Fetches weather data for the required date range
- Updates records with corresponding weather information
- Uses Scottsdale, Arizona coordinates (33.7591, -111.7270) by default(my home!)
- Processes up to 8000 records per run to manage API limits

## Automation
For automated daily collection, set up a cron job or scheduled task to run both scripts in sequence:
```bash
# Example cron entry for daily execution at 6 AM
0 6 * * * /path/to/python /path/to/srp-daily.py && /path/to/python /path/to/weather.py
```