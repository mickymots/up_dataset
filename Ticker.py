from multiprocessing import Pool
from apitools import *
import pandas as pd

class Ticker:

    limit = 50000
    def __init__(self, ticker, day, apiKey):
        self.ticker = ticker
        self.day = day
        self.apiKey = apiKey

    def get_record_for_min(self):
        (start, start_ts, end_ts) = self.day
        per_min_volume_data = call_api(
            start_ts, end_ts, 1, 'minute', self.ticker, 50000, self.apiKey)
        max_1_min_volume = get_max_minute_volume(per_min_volume_data)

        if max_1_min_volume:
            return {'v_1min_max': max_1_min_volume['v']}
        else:
            return {'v_1min_max': 0}


    def get_trade_record_for_ts(self, timestamp):
        (start, start_ts, end_ts)  = self.day
        try:
            return call_trades_api(start, self.ticker, apiKey=self.apiKey, timestamp=timestamp)
        except Exception as e:
            print(e)

    

    def get_trades_record_for_day(self):
        (start, start_ts, end_ts)  = self.day
        print(f'call trade records')

        more_pages = True
        timestamp = None

        total_record = 0
        total_volume = 0
        max_lo = 0
        lo_c = []
        exchange = ''

        while more_pages:
            data = self.get_trade_record_for_ts(timestamp)
            # print(f"trade data = {data['results_count']}")
            if data and 'results_count' in data:
                results_count = data['results_count']
                
                # total record
                total_record += results_count
                try:
                                    #max lot in page  LO s , LO Ex I LO con  c
                    df = pd.json_normalize(data['results'])

                    total_volume += df['s'].sum()
                    if max_lo < df['s'].max():
                        max_lo = df['s'].max()
                        maxIdx = df['s'].idxmax()
                        exchange = df.iloc[maxIdx]['i']
                        lo_c = df.iloc[maxIdx]['c']
                except Exception as e:
                    print(e)
                more_pages = Ticker.limit <= results_count
                if more_pages:
                    last_record = data['results'][-1]
                    timestamp = last_record['t']
            else:
                more_pages = False
        
        average_order = total_volume/total_record
        return {'Average Order': average_order, 'Median Order': 100,  'LO_Size': max_lo, "LO_Exchange": exchange, 'LO_Condition': lo_c}




    def get_record_for_day(self):
        (start, start_ts, end_ts) = self.day
        per_min_volume_data = self.get_record_for_min()

        per_day_volume_data = call_api(
            start_ts, end_ts, 1, 'day', self.ticker, 50000, self.apiKey)

        
        record_exists_for_day = 'resultsCount' in per_day_volume_data
        # print(f"record exists for day = {record_exists_for_day}")

        if record_exists_for_day:
            trade_record = self.get_trades_record_for_day()
            print(f'trade = {trade_record}')
            return {**per_day_volume_data['results'][0], **per_min_volume_data, **trade_record}
        else:
            print(f'no data - {per_day_volume_data}')

    def build_dataset(self):
        data =  self.get_record_for_day()
        print(f'data - {data}')
        return data
