from multiprocessing import Pool
from apitools import *


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
        return call_trades_api(start, self.ticker, apiKey=self.apiKey, timestamp=timestamp)
    

    def get_trades_record_for_day(self):
        (start, start_ts, end_ts)  = self.day

        more_pages = True
        timestamp = None
        count = 0
        while more_pages:
            data = self.get_trade_record_for_ts(timestamp)
            # print(f"trade data = {data['results_count']}")
            if('results_count' in data):
                results_count = data['results_count']
                more_pages = Ticker.limit <= results_count

                if(results_count):
                    count += 1
                    last_record = data['results'][-1]
                    timestamp = last_record['t']
            else:
                more_pages = False
        
        # print(f'total pages = {count} on {start} for {self.ticker}')
        return count

    def get_record_for_day(self):
        (start, start_ts, end_ts) = self.day
        per_min_volume_data = self.get_record_for_min()

        per_day_volume_data = call_api(
            start_ts, end_ts, 1, 'day', self.ticker, 50000, self.apiKey)

        if per_day_volume_data['resultsCount']:
            trade_record = self.get_trades_record_for_day()
            return {**per_day_volume_data['results'][0], **per_min_volume_data}

    def build_dataset(self):
        return self.get_record_for_day()
