from multiprocessing import Pool
from apitools import *
import asyncio


class Ticker:
    limit = 50000
    def __init__(self, ticker, day, apiKey):
        self.ticker = ticker
        self.day = day
        self.apiKey = apiKey


    def get_record_for_min(self):
        (start, start_ts, end_ts)  = self.day
        return call_volume_api(start_ts, end_ts, 1, 'minute',self.ticker, 50000, self.apiKey)
        
    def get_volume_record_for_day(self):
        (start, start_ts, end_ts)  = self.day
        return call_volume_api(start_ts, end_ts, 1, 'day', self.ticker, 50000, self.apiKey)
        

    def build_record_for_min(self, per_min_volume_data_task):
        per_min_volume_data = per_min_volume_data_task
        print(f"{per_min_volume_data} --- per min record")
        max_1_min_volume = get_max_minute_volume(per_min_volume_data)

        if max_1_min_volume:
            return {'v_1min_max': max_1_min_volume['v'] }
        else:
            return  {'v_1min_max': 0}

    


    async def get_record_for_day(self):
        (start, start_ts, end_ts)  = self.day

        per_min_volume_data_task = self.get_record_for_min()        
        per_day_volume_data_task = self.get_volume_record_for_day()

        # per_day_trade_data = self.get_trades_record_for_day()

        per_min_volume = await per_min_volume_data_task
        per_day_record = await per_day_volume_data_task
        
        per_min_record = self.build_record_for_min(per_min_volume)

        print(f"{per_day_record}-per_day_record")

        if per_day_record['resultsCount']:
            day_volume_record =  {**per_day_record['results'][0] , **per_min_record, **{'symbol':self.ticker}}
            
            print(f'day_volume_record = {day_volume_record}')
            
            # await per_day_trade_data
            return day_volume_record
        
    
    async def get_trade_record_for_ts(self, timestamp):
        (start, start_ts, end_ts)  = self.day
        return asyncio.create_task(call_trades_api(start, self.ticker, apiKey=self.apiKey, timestamp=timestamp))
        

    async def get_trades_record_for_day(self):
        (start, start_ts, end_ts)  = self.day
        

        more_pages = True
        timestamp = None
        count = 0
        while more_pages:
            trade_call = await self.get_trade_record_for_ts(timestamp)
            response = await trade_call
            data = await response
            if('results_count' in data):
                results_count = data['results_count']
                more_pages = Ticker.limit <= results_count

                if(results_count):
                    count += 1
                    last_record = data['results'][-1]
                    timestamp = last_record['t']
            else:
                more_pages = False
        
        print(f'total pages = {count} on {start} for {self.ticker}')
        return count

    
    async def build_dataset(self):
        # with Pool(4) as p:
        #     data = list(filter(lambda v: v , p.map(self.get_record_for_day, getTimeRange(self.days))))
        #     return data
        
        print(f'waiting for {self.ticker} dataset')
        dataset = await self.get_record_for_day()
        # await dataset
        print(f'dataset for {self.ticker} --> {dataset}')
        return dataset
            
        
            