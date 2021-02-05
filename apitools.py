# from urllib import request
from datetime import datetime, timedelta, date
import csv
from functools import reduce
import json
import requests
import asyncio
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

BASE_URL = 'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{timespan}/{multiplier}/{start}/{end}?unadjusted=true&sort=asc&limit=50000&apiKey={apiKey}'
TRADE_API = 'https://api.polygon.io/v2/ticks/stocks/trades/{ticker}/{start}?limit=50000&apiKey={apiKey}&reverse=false'


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


async def call_service(url):
    try:
        with requests_retry_session().get(url) as response:
            return json.loads(response.text)
    except Exception as e:
           print(e)

def call_volume_api(start, end, timespan, mutliplier, ticker, limit, apiKey):
    url = BASE_URL.format(ticker=ticker, multiplier=mutliplier, timespan=timespan, start=start, end=end, apiKey=apiKey )
    
    print(f'api_call_volume {ticker}')
    return asyncio.create_task(call_service(url))


def call_trades_api(start, ticker, apiKey, timestamp):
    url = TRADE_API.format(ticker=ticker, start=start, apiKey=apiKey )
    if(timestamp):
        url = url + f'&timestamp={timestamp}'
    print(f'api_call_trade {ticker} - {url}')
    return asyncio.create_task(call_service(url))
    

def getTimeRange(date_range):
    start_base = datetime(2021, 1, 31, 9,0,0) 
    end_base = datetime(2021, 1, 31, 23,59, 59)

    for i in range(date_range):
        start_base_day = start_base - timedelta(days=i)
        end_base_day = end_base - timedelta(days=i)

        start = (int)(datetime.timestamp(start_base_day) * 1000)
        end = (int)(datetime.timestamp(end_base_day) * 1000)
        
        yield (start,end)

def getDateTuple(day):
    start_base = datetime(2021, 1, 31, 9,0,0) - timedelta(days=day)

    start_ts = (int)(datetime.timestamp(start_base) * 1000)
    end_ts = (int)(datetime.timestamp(start_base) * 1000)
    start = start_base.strftime("%Y-%m-%d")

    return (start, start_ts, end_ts)



def get_max_minute_volume(data):
    if('results' in data):
        results = data['results']
        max_volume = reduce(reducer_fn, results)
        return max_volume


def record_generator(data, ticker):
    timestamp = datetime.fromtimestamp((int)(data['t']/1000))
    record = (timestamp.strftime('%Y%m%d'), ticker,  data['v'], data['vw'], data['0'], data['c'], data['h'] , data['l'], data['v_1min_max'])
    return record

def reducer_fn(item1, item2):
    return item1 if item1['v'] >=  item2['v'] else item2