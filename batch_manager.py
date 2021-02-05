import csv
import pandas as pd
from max_minute_vol_parallel import main as batch_runner
import asyncio

async def run_batch(batch, day):
    await batch_runner(batch, day)



async def main():

    csv_df = pd.read_csv('./tickers.csv', header=0, usecols=['symbol'], chunksize=25, iterator=True)

    i = 0
    j = 1

    # reading in batch/chunks of 10 tickers
    for df in csv_df:

        df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
        df.index += j
        i+=1
        batch = []
        for ind in df.index:
            # task = asyncio.create_task(nested())
            batch.append(df['symbol'][ind])


        print(batch)
        task = asyncio.create_task(run_batch(batch, 4))
        await task

asyncio.run(main())