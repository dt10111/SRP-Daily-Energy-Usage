from datetime import datetime, timedelta
from srpenergy.client import SrpEnergyClient
import MySQLdb
import array
import os
from dotenv import load_dotenv

load_dotenv()
    
mydb = MySQLdb.Connection(
host=os.getenv('BI_HOST'),
user=os.getenv('BI_USER'),
password=os.getenv('BI_PASS'),
port=3306,
db=os.getenv('GEN_DB_NAME')
)
cursor = mydb.cursor()
i = 0
x = i+1
print(i)    
while i < 1: #to backfill, adjust the 2 here to the range size and the i to a relative start date. Otherwise this loops once for a day's worth of data
    accountid = os.getenv('SRP_ACCOUNT')
    try:
        username =os.getenv('SRP_USER')
        password = os.getenv('SRP_PASS')
        end_date = datetime.now() - timedelta(days=i)
        start_date = datetime.now() - timedelta(days=x)

        client = SrpEnergyClient(accountid, username, password)
        usage = client.usage(start_date, end_date)

        date, hour, isodate, kwh, cost = usage[0]
        for row in usage:
            my_list = []
            rawTime = row[2]
            formatTime = datetime.strptime(rawTime, '%Y-%m-%dT%H:%M:%S')
            my_list.append(row[0])
            my_list.append(row[1])
            my_list.append(row[2])
            my_list.append(row[3])
            my_list.append(row[4])
            my_list.append(formatTime)
            print(my_list)
            cursor.execute('INSERT INTO srp(date, hour, isotime, kwh, cost, datetime)VALUES(%s, %s, %s, %s, %s, %s)',my_list)
        mydb.commit()
        i = i + 1
        x = i + 1
    except Exception as e:    
        print(e)
cursor.close()
