from datetime import datetime, timedelta
from srpenergy.client import SrpEnergyClient
import MySQLdb
import pandas as pd
import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('srp_data_collection.log'),
        logging.StreamHandler()
    ]
)

load_dotenv()

class SrpDataManager:
    def __init__(self):
        self.db_connection = None
        self.cursor = None
        self.setup_database()
        
    def setup_database(self):
        """Initialize database connection"""
        try:
            self.db_connection = MySQLdb.Connection(
                host=os.getenv('BI_HOST'),
                user=os.getenv('BI_USER'),
                password=os.getenv('BI_PASS'),
                port=3306,
                db=os.getenv('GEN_DB_NAME')
            )
            self.cursor = self.db_connection.cursor()
            logging.info("Database connection established")
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise
    
    def get_existing_datetime_records(self):
        """Query database to get all existing date/time combinations"""
        try:
            self.cursor.execute("""
                SELECT DISTINCT DATE(datetime) as date, HOUR(datetime) as hour, datetime
                FROM srp 
                ORDER BY datetime
            """)
            results = self.cursor.fetchall()
            
            existing_combos = set()
            for date, hour, full_datetime in results:
                date_str = date.strftime('%Y-%m-%d')
                existing_combos.add(f"{date_str}-{hour:02d}")
            
            logging.info(f"Found {len(existing_combos)} existing date/hour combinations")
            return existing_combos, results
            
        except Exception as e:
            logging.error(f"Error querying existing records: {e}")
            return set(), []
    
    def get_date_range_from_db(self):
        """Get the full date range from existing data"""
        try:
            self.cursor.execute("""
                SELECT MIN(DATE(datetime)) as min_date, MAX(DATE(datetime)) as max_date
                FROM srp
            """)
            result = self.cursor.fetchone()
            
            if result and result[0] and result[1]:
                return result[0], result[1]
            else:
                # Default range if no data exists
                return datetime.now().date() - timedelta(days=30), datetime.now().date()
                
        except Exception as e:
            logging.error(f"Error getting date range: {e}")
            return datetime.now().date() - timedelta(days=30), datetime.now().date()
    
    def find_missing_datetime_combinations(self, start_date=None, end_date=None):
        """Find all missing date/hour combinations in the specified range"""
        
        if not start_date or not end_date:
            start_date, end_date = self.get_date_range_from_db()
        
        # Convert to datetime objects if they're date objects
        if hasattr(start_date, 'date'):
            start_date = start_date.date()
        if hasattr(end_date, 'date'):
            end_date = end_date.date()
            
        logging.info(f"Checking for missing data between {start_date} and {end_date}")
        
        existing_combos, _ = self.get_existing_datetime_records()
        
        missing_combinations = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            for hour in range(24):
                combo_key = f"{date_str}-{hour:02d}"
                
                if combo_key not in existing_combos:
                    missing_dt = datetime.combine(current_date, datetime.min.time()) + timedelta(hours=hour)
                    missing_combinations.append({
                        'date': current_date,
                        'hour': hour,
                        'datetime': missing_dt,
                        'date_str': date_str,
                        'combo_key': combo_key
                    })
            
            current_date += timedelta(days=1)
        
        logging.info(f"Found {len(missing_combinations)} missing date/hour combinations")
        return missing_combinations
    
    def analyze_data_gaps(self, start_date=None, end_date=None):
        """Analyze and report on data gaps"""
        missing_combos = self.find_missing_datetime_combinations(start_date, end_date)
        
        if not missing_combos:
            logging.info("No missing data found - database is complete!")
            return
        
        # Group by date to analyze patterns
        missing_by_date = {}
        for combo in missing_combos:
            date_str = combo['date_str']
            if date_str not in missing_by_date:
                missing_by_date[date_str] = []
            missing_by_date[date_str].append(combo['hour'])
        
        complete_days = 0
        partial_days = []
        missing_days = []
        
        for date_str, missing_hours in missing_by_date.items():
            if len(missing_hours) == 24:
                missing_days.append(date_str)
            else:
                partial_days.append({'date': date_str, 'missing_hours': sorted(missing_hours)})
        
        # Count complete days
        start_date, end_date = self.get_date_range_from_db()
        total_days = (end_date - start_date).days + 1
        days_with_gaps = len(missing_by_date)
        complete_days = total_days - days_with_gaps
        
        # Print analysis
        print("\n" + "="*50)
        print("DATA GAP ANALYSIS")
        print("="*50)
        print(f"Date range analyzed: {start_date} to {end_date}")
        print(f"Total days in range: {total_days}")
        print(f"Complete days (24 hours): {complete_days}")
        print(f"Days with missing data: {days_with_gaps}")
        print(f"Completely missing days: {len(missing_days)}")
        print(f"Partially missing days: {len(partial_days)}")
        print(f"Total missing records: {len(missing_combos)}")
        
        if missing_days:
            print(f"\nCompletely missing days ({len(missing_days)}):")
            for day in sorted(missing_days)[:10]:  # Show first 10
                print(f"  {day}")
            if len(missing_days) > 10:
                print(f"  ... and {len(missing_days) - 10} more")
        
        if partial_days:
            print(f"\nPartially missing days ({len(partial_days)}):")
            for day_info in sorted(partial_days, key=lambda x: x['date'])[:10]:  # Show first 10
                hours = day_info['missing_hours']
                hour_ranges = self._compress_hour_ranges(hours)
                print(f"  {day_info['date']}: missing hours {hour_ranges}")
            if len(partial_days) > 10:
                print(f"  ... and {len(partial_days) - 10} more")
        
        return missing_combos
    
    def _compress_hour_ranges(self, hours):
        """Convert list of hours to compressed ranges (e.g., [0,1,2,5,6] -> '0-2, 5-6')"""
        if not hours:
            return ""
        
        ranges = []
        start = hours[0]
        prev = hours[0]
        
        for hour in hours[1:] + [None]:  # Add None to trigger final range
            if hour is None or hour != prev + 1:
                if start == prev:
                    ranges.append(str(start))
                else:
                    ranges.append(f"{start}-{prev}")
                if hour is not None:
                    start = hour
            if hour is not None:
                prev = hour
        
        return ', '.join(ranges)
    
    def fetch_missing_data(self, missing_combinations, batch_size=24):
        """Fetch missing data from SRP API and insert into database"""
        
        if not missing_combinations:
            logging.info("No missing data to fetch")
            return
        
        accountid = os.getenv('SRP_ACCOUNT')
        username = os.getenv('SRP_USER')
        password = os.getenv('SRP_PASS')
        
        # Group missing combinations by date for efficient API calls
        missing_by_date = {}
        for combo in missing_combinations:
            date_key = combo['date_str']
            if date_key not in missing_by_date:
                missing_by_date[date_key] = []
            missing_by_date[date_key].append(combo)
        
        total_dates = len(missing_by_date)
        successful_fetches = 0
        failed_fetches = 0
        
        for i, (date_str, date_combos) in enumerate(sorted(missing_by_date.items())):
            try:
                logging.info(f"Processing {date_str} ({i+1}/{total_dates}) - {len(date_combos)} missing hours")
                
                # Convert date string back to date object
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                start_date = datetime.combine(date_obj, datetime.min.time())
                end_date = start_date + timedelta(days=1)
                
                # Fetch data from SRP API
                client = SrpEnergyClient(accountid, username, password)
                usage = client.usage(start_date, end_date)
                
                if not usage:
                    logging.warning(f"No data returned for {date_str}")
                    failed_fetches += 1
                    continue
                
                # Insert data into database
                records_inserted = 0
                for row in usage:
                    date_val, hour_val, isodate_val, kwh_val, cost_val = row
                    
                    # Parse the ISO date to get a proper datetime object
                    format_time = datetime.strptime(isodate_val, '%Y-%m-%dT%H:%M:%S')
                    
                    # Check if this specific hour was missing
                    hour_num = format_time.hour
                    combo_key = f"{date_str}-{hour_num:02d}"
                    
                    missing_hours = [combo['hour'] for combo in date_combos]
                    if hour_num in missing_hours:
                        record_data = [
                            date_val,
                            hour_val, 
                            isodate_val,
                            kwh_val,
                            cost_val,
                            format_time
                        ]
                        
                        self.cursor.execute(
                            'INSERT INTO srp(date, hour, isotime, kwh, cost, datetime) VALUES(%s, %s, %s, %s, %s, %s)',
                            record_data
                        )
                        records_inserted += 1
                
                self.db_connection.commit()
                logging.info(f"Inserted {records_inserted} records for {date_str}")
                successful_fetches += 1
                
            except Exception as e:
                logging.error(f"Error processing {date_str}: {e}")
                failed_fetches += 1
                continue
        
        logging.info(f"Data fetching complete: {successful_fetches} successful, {failed_fetches} failed")
    
    def run_complete_gap_analysis_and_fill(self, start_date=None, end_date=None, fill_gaps=False):
        """Complete workflow: analyze gaps and optionally fill them"""
        
        print("Starting comprehensive data gap analysis...")
        
        # Step 1: Analyze current gaps
        missing_combos = self.analyze_data_gaps(start_date, end_date)
        
        if not missing_combos:
            return
        
        # Step 2: Optionally fill gaps
        if fill_gaps:
            response = input(f"\nFound {len(missing_combos)} missing records. Fetch from SRP API? (y/n): ")
            if response.lower() == 'y':
                print("Fetching missing data from SRP API...")
                self.fetch_missing_data(missing_combos)
                print("Gap filling complete!")
            else:
                print("Skipping data fetch.")
        else:
            print(f"\nTo fill these gaps, run with fill_gaps=True")
            print("Or call fetch_missing_data() manually")
    
    def close_connection(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.db_connection:
            self.db_connection.close()
        logging.info("Database connection closed")


# Example usage
if __name__ == "__main__":
    manager = SrpDataManager()
    
    try:
        # Option 1: Just analyze gaps without filling
        manager.run_complete_gap_analysis_and_fill(fill_gaps=False)
        
        # Option 2: Analyze and fill gaps (uncomment to use)
        # manager.run_complete_gap_analysis_and_fill(fill_gaps=True)
        
        # Option 3: Analyze specific date range
        # start = datetime(2025, 3, 1).date()
        # end = datetime(2025, 5, 31).date()
        # manager.run_complete_gap_analysis_and_fill(start, end, fill_gaps=True)
        
    finally:
        manager.close_connection()
