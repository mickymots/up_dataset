from urllib import request
from datetime import datetime, timedelta, date
import csv
from functools import reduce
import json
import requests
encoding = 'utf-8'

apiKey = 'B1El3hA6pObNMagdrpbn4pLk_YXpm4cY'
BASE_URL = 'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{timespan}/{multiplier}/{start}/{end}?unadjusted=true&sort=asc&limit=50000&apiKey={apiKey}'

def call_api(start, end, timespan, mutliplier, ticker, limit ):
    url = BASE_URL.format(ticker=ticker, multiplier=mutliplier, timespan=timespan, start=start, end=end, apiKey=apiKey )
    with requests.get(url) as response:
        return json.loads(response.text)


def getDateRange(date_range):
    start_base = datetime(2021, 1, 31) 
    end_base = datetime(2021, 1, 31, 23,59, 59)

    for i in range(date_range):
        start_base_day = start_base - timedelta(days=i)
        end_base_day = end_base - timedelta(days=i)

        start = (int)(datetime.timestamp(start_base_day) * 1000)
        end = (int)(datetime.timestamp(end_base_day) * 1000)
        
        yield (start,end)

def get_max_minute_volume(data):
    if('results' in data):
        results = data['results']
        max_volume = reduce(reducer_fn, results)
        return max_volume

def record_generator(data, ticker):
    timestamp = datetime.fromtimestamp((int)(data['t']/1000))
    record = (timestamp.strftime('%Y%m%d'), ticker,  data['v'])
    return record

def reducer_fn(item1, item2):
    return item1 if item1['v'] >=  item2['v'] else item2

def main():
    days = int(input('Enter # of days to query for? '))
    aapl_file = open('AAPL_1min.csv', 'a')
    csv_out=csv.writer(aapl_file)
    for time_range in getDateRange(days):
        start, end = time_range
        per_min_volume_data = call_api(start,end, 1, 'minute','AAPL', 50000)

        max_volume = get_max_minute_volume(per_min_volume_data)

        if(max_volume):
            csv_out.writerow(record_generator(max_volume, 'AAPL'))
            print( record_generator(max_volume, 'AAPL'))
            

if __name__ == '__main__':
    main()