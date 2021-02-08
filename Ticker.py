from multiprocessing import Pool
from apitools import *
import pandas as pd
import logging

logging.basicConfig(filename='log/ticker.log', level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

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
            return {'1mVolume': max_1_min_volume['v'], 'Ticker': self.ticker}
        else:
            return {'1mVolume': 0, 'Ticker': self.ticker}


    def get_trade_record_for_ts(self, timestamp):
        (start, start_ts, end_ts)  = self.day
        try:
            return call_trades_api(start, self.ticker, apiKey=self.apiKey, timestamp=timestamp)
        except Exception as e:
            log.error(f'trade call - {e}')

    def get_trades_record_for_day(self):
        more_pages = True
        timestamp = None        
        results = {}
        while more_pages:
            data = self.get_trade_record_for_ts(timestamp)
            if data and 'results_count' in data:
                results_count = data['results_count']

                if 'results' in results:
                    results['results'].append(data['results'])
                else:
                    results = data

                more_pages = Ticker.limit <= results_count
                if more_pages:
                    last_record = data['results'][-1]
                    timestamp = last_record['t']
            else:
                more_pages = False
        
        trade_data = self.compute_lot_stats(results)
        if trade_data:
            (avg, median_order, max_lo, exchange, lo_c, lo_per_vol) = trade_data
        else:
            log.warn(f'No trade data for {self.day} {self.ticker}')
            
        trade_json = {'Average Order': avg, 'Median Order': median_order,  'LO_Size': max_lo, "LO_Exchange": exchange, 'LO_Condition': lo_c, 'lo_per_vol': lo_per_vol*100}
        
        return trade_json

    # Compute Lot Avg, Median, Max, LO % Volunme
    def compute_lot_stats(self, data):
        try:
            df = pd.json_normalize(data['results'])

            total_volume= df['s'].sum()            
            max_lo = df['s'].max()
            median_order = df['s'].median()
            avg = df['s'].mean()
            maxIdx = df['s'].idxmax()
            exchange = df.iloc[maxIdx]['i']
            lo_c = df.iloc[maxIdx]['c']
            
            return (avg, median_order, max_lo, exchange, lo_c, max_lo/total_volume)
        except Exception as e:
            log.warn(f'compute_lot_stats for data {self.day} - {self.ticker} - {e}')

    def get_record_for_day(self):
        (start, start_ts, end_ts) = self.day
        per_min_volume_data = self.get_record_for_min()

        per_day_volume_data = call_api(
            start_ts, end_ts, 1, 'day', self.ticker, 50000, self.apiKey)
        
        record_exists_for_day = 'resultsCount' in per_day_volume_data

        if record_exists_for_day:
            trade_record = self.get_trades_record_for_day()            
            return {**{'date': start},**per_min_volume_data, **trade_record, **per_day_volume_data['results'][0]}
        else:
            log.info(f'no data - {per_day_volume_data}')

    def build_dataset(self):
        data =  self.get_record_for_day()
        return data
