
import glob
import pandas as pd
import logging

import asyncio

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


all_files = glob.glob("/home/amit/projects/python-workspace/upwork-dataset/short_data/*.txt")

li = []

async def get_short_data():
    logging.info('-- loading short interest ---')
    
        
    for filename in all_files:
        df = pd.read_csv(filename, sep='|', index_col=None, header=0)
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)
    df.rename(columns={"Date": "date", "Symbol": "Ticker"}, inplace=True)
    return df

async def read_csv():
    df = pd.read_csv('short.csv', sep=',', index_col=None, header=0)
    return df

async def main():
    logging.info('-- running main ---')
    task = asyncio.create_task(read_csv())
    df = await task

    
    df.date = df.date.astype(str)

    # df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')



    # print(df)
    # print(df)
    # df.to_csv('short_try.csv', index=False)
    
    return df

if __name__ == "__main__":
    asyncio.run(main())