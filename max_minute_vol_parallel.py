from Ticker import Ticker
from datetime import datetime
import csv
from os import getenv
from apitools import getDateTuple
import asyncio

# write the data to file
def write_to_file(data, file_name):
    csv_columns = data.keys()
    csv_out=csv.DictWriter(open(file_name, 'a'), fieldnames=csv_columns)
    csv_out.writeheader()
    csv_out.writerow(data)

#build output file name for the ticker
def build_file_name(ticker, day):
    file_version = (str)(datetime.today().timestamp())[:11]
    file_name = f'data/{ticker}_1min_max_volume_{file_version}.csv'
    return file_name

# build dataset for given number of days and the stock
async def build_dataset(ticker, day, apiKey):
    # print(f'build dataset called for = {ticker}')
    ticker = Ticker(ticker, day, apiKey)
    return await asyncio.create_task(ticker.build_dataset())

# main functions to build the dataset
async def get_dataset(ticker, day):
    # print(f'get dataset called for = {ticker}')
    if(getenv('apiKey')):
        apiKey = getenv('apiKey')

        data = await asyncio.create_task(build_dataset(ticker=ticker, day=day, apiKey=apiKey))
        if data:
            write_to_file(data, file_name= build_file_name(ticker, day))
        else:
            raise ValueError(f'No record for {day}')

    else:
        raise EnvironmentError('set the apiKey env variable')



async def main(batch, day_offset):
    day = getDateTuple(day_offset)
    for ticker in batch:
        await asyncio.gather(
            asyncio.create_task(get_dataset(ticker, day))
        )


if __name__ == '__main__':
    try:
        day_offset = int(input('Enter of day to query for?  '))
        
        
        asyncio.run(main(batch, day_offset))


    except ValueError as error:
        print(error)
    except EnvironmentError as error:
        print(error)
    
