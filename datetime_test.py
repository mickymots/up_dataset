
from datetime import datetime, timedelta

now = datetime.today()
print(now)
#datetime(year, month, day)
a = datetime(2018, 11, 28) - timedelta(days=5)
print(a)

# datetime(year, month, day, hour, minute, second, microsecond)
b = datetime(2017, 11, 28, 23, 59, 59) - timedelta(days=5)
print(b)


timestamp = (int)(datetime.timestamp(a) * 1000)
print("timestamp =", timestamp)

timestamp = date.fromtimestamp(1326244364)
print("Date =", timestamp)