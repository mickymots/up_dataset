import csv
import pandas as pd
from dataset_builder import main as builder
from os import getenv
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import time

from datetime import datetime, timedelta
import os
import numpy as numpy

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
headers = ['date','1mVolume','Ticker','Average Order','Median Order','LO_Size','LO_Exchange','LO_Condition','lo_per_vol','v','vw','o','c','h','l','t', 'High Break']

#date,1mVolume,Ticker,Average Order,Median Order,LO_Size,LO_Exchange,LO_Condition,lo_per_vol,v,vw,o,c,h,l,t,n
#set the output file name
output_file = 'dataset.csv'

def run_batch(days, ticker):
    """
        days_list is a simple list of number from 1 to n e.g. [0,1,2,3,4,5].  These number represent day delta from a base start date.
        i.e. - Query for day= base_start_date and after that query loop for n previous days.
    """
    days_list = [i for i in range(days)]
    
    with ThreadPoolExecutor(max_workers=40) as executor:
        fn = partial(builder, ticker)
        executor.map(fn, days_list)
        executor.shutdown(wait=True)


def main():
    ts_start = time()
    batch_size = int(input('Enter Batch Size : '))
    days_to_query = int(input('Enter Days to Query : '))
    
    csv_df = pd.read_csv('./source/tickers.csv', header=0, usecols=['symbol'], chunksize=batch_size, iterator=True)
    prep_output_file()
    execute_batch(csv_df, days_to_query)
    
    process_batch()    
    logging.info('Total Processing Took %s seconds', time() - ts_start)   



# prepare the output file where the dataset records will be written
def prep_output_file():
    if os.path.exists(f'data/{output_file}'):
        file_version = int(round(datetime.today().timestamp()))
        os.rename(f'data/{output_file}', f"data/dataset_{file_version}.csv")
   
    with open(f'data/{output_file}', 'w', newline='') as outcsv:
        writer = csv.writer(outcsv)
        writer.writerow(headers)

# prepare batch processing output file
def prep_process_file():
    
    if os.path.exists(f'data/processed_{output_file}'):
        file_version = int(round(datetime.today().timestamp()))
        os.rename(f'data/processed_{output_file}', f'data/processed_{output_file}_{file_version}.csv')
    


# execute the batch of tickers for given numbers of days
def execute_batch(batch_dataframe, days_to_query):
    ts_batch_start = time()
    i = 0
    j = 1
    batch = []
    for df in batch_dataframe:

        df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
        df.index += j
        i+=1
        
        for ind in df.index:
            batch.append(df['symbol'][ind])

        logging.info(batch)
        run_batch_fn = partial(run_batch, days_to_query)
        for ticker in batch:
            run_batch_fn(ticker)
    logging.info('Batch Query Took %s seconds', time() - ts_batch_start)




# process the batch to add breakout data
def process_batch():
    ts_batch_start = time()
    prep_process_file()
    
    csv_df = pd.read_csv(f'data/{output_file}', header=0)
    
    csv_df.sort_values(by='t', inplace=True, ascending=False)
    numpy_list = csv_df.to_numpy()

    calcualte_breakouts(numpy_list)
    try:
        pd.DataFrame(numpy_list).to_csv(f'data/processed_{output_file}', header=headers, index=None)
    except Exception as e:
        logging.error(e)
    logging.info('Batch Processing Took %s seconds', time() - ts_batch_start)  
    
     
    

# calculate the breakouts
def calcualte_breakouts(numpy_list):
    i = 0
    
    for item in numpy_list:
        lot_size = item[5]
        volume_1min = item[1]
        volume = item[9]
        
        fromDt = getPDDate(item[0])
        found_lot_dt = getPDDate(item[0])
        found_vol_1min_dt = getPDDate(item[0])
        found_vol_dt = getPDDate(item[0])

        # lot size breakout
        try:
            found_lot_dt = next(getPDDate(x[0]) for x in numpy_list[i+1:] if x[5] >= lot_size)
        
        except StopIteration as e:
            logging.warning('highest of rest of the records')
            found_lot_dt = getPDDate(numpy_list[-1][0])
        except Exception as e:
            logging.error("error in breakout calculation", e)
        
        # 1 min volume breakoit
        try:
            found_vol_1min_dt = next(getPDDate(x[0]) for x in numpy_list[i+1:] if x[1] >= volume_1min)
        
        except StopIteration as e:
            logging.warning('highest of rest of the records')
            found_vol_1min_dt = getPDDate(numpy_list[-1][0])
        except Exception as e:
            logging.error("error in breakout calculation", e)
            
        # breakout volume 
        try:
            found_vol_dt = next(getPDDate(x[0]) for x in numpy_list[i+1:] if x[9] >= volume)
        
        except StopIteration as e:
            logging.warning('highest of rest of the records')
            found_vol_dt = getPDDate(numpy_list[-1][0])
        except Exception as e:
            logging.error("error in breakout calculation", e)
            

            
        days = numpy.busday_count(found_lot_dt, fromDt)
        item.put(-1,(days))
        
        i += 1
    
def getPDDate(value):
    return pd.to_datetime(value,format="%Y-%m-%d").date()
    

if __name__ == '__main__':
    if(getenv('apiKey')):
        full = input('Enter f for full query operation else press any key : ')
        if full == 'f':
            main()
        else:
            process_batch()
        # main()
    else:
        raise EnvironmentError('set the apiKey env variable')
