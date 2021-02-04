from apitools import *
from os import getenv

# Check if the apiKey is set if not raise an error
# take inputs from user
# call the build dataset function
def main():
    if(getenv('apiKey')):
        days = int(input('Enter # of days to query for? '))
        ticker = input('Enter ticker of the stock ? ')
        apiKey = getenv('apiKey')
        build_dataset(days, ticker, apiKey)

    else:
        raise EnvironmentError('set the apiKey env variable')
    
# call REST API for given number of days and write output to CSV file   
def build_dataset(days, ticker, apiKey):
    file_name = f'{ticker}_1min_max_volume.csv'
    csv_out=csv.writer(open(file_name, 'a'))
    
    for (start, end) in getTimeRange(days):
        
        per_min_volume_data = call_api(start,end, 1, 'minute',ticker, 50000, apiKey)

        max_volume = get_max_minute_volume(per_min_volume_data)

        if(max_volume):
            csv_out.writerow(record_generator(data=max_volume, ticker=ticker))


if __name__ == '__main__':
    try:
        main()
    except EnvironmentError as error:
        print(error)