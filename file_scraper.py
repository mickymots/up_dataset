import datetime
from urllib import request
encoding = 'utf-8'

def open_short_api(date):
    url = f'http://regsho.finra.org/CNMSshvol{date}.txt'
    try:
        with request.urlopen(url) as response:
            data = response.read()
            return data

    except:
        print(f'date = {date}')

def getDate(date_range):
    base = datetime.datetime.today() - datetime.timedelta(days=5)
    result =[]
    for i in range(date_range):
        day = base - datetime.timedelta(days=i)
        year = str(day.year)
        month = str(day.month).rjust(2, '0')
        date = str(day.day).rjust(2, '0')
        result.append(year+month+date)
    return result


def getShort_data(day):
    
    short_data =  open_short_api(day)
    if short_data:
        try:
            short_data_str = short_data.decode(encoding)
            sink = open(f'short_data/short{day}.txt', 'w')
            sink.write(short_data_str)
        except Exception as e:
            print(e)

from concurrent.futures import ThreadPoolExecutor

def main():
    date_range = getDate(750)
    with ThreadPoolExecutor(max_workers=40) as executor:
        executor.map(getShort_data, date_range)
        executor.shutdown(wait=True)


main()