import datetime as datetime
from dateutil.relativedelta import relativedelta, FR
from dateutil import rrule
import time
import json
import numpy as np

def unix_ms_to_datetime(unix_ms):
    return datetime.datetime.utcfromtimestamp(unix_ms / 1000.0)

def datetime_to_unix_ms(dt):
    return int(dt.timestamp() * 1000)

def timeit(f):

    def timed(*args, **kw):

        ts = time.time()
        result = f(*args, **kw)
        te = time.time()

        # print('func:%r args:[%r, %r] took: %2.4f sec' %(f.__name__, args, kw, te-ts))
        print('func:%r took: %2.4f sec' %(f.__name__, te-ts))
        return result

    return timed


def build_option_expiries(date):
    """
    Given a date, returns a list of future option expiration dates. The list includes the next 3 days
    for daily option expiries, the next 3 Fridays for weekly option expiries, the next 3 last month Fridays
    for monthly option expiries, and the next 3 quarters' last Fridays of the month for quarterly option
    expiries.
    
    Args:
    - date (datetime.date): The starting date to calculate option expiries from.
    
    Returns:
    - List[datetime.date]: A list of datetime.date objects representing future option expiration dates.
    
    Example:
    >>> date = datetime.date(2023, 4, 6)
    >>> expiries = build_option_expiries(date)
    >>> print(expiries)
    [datetime.date(2023, 4, 7), datetime.date(2023, 4, 8), datetime.date(2023, 4, 14), datetime.date(2023, 4, 21), 
    datetime.date(2023, 4, 28), datetime.date(2023, 5, 5), datetime.date(2023, 5, 31), datetime.date(2023, 6, 30), 
    datetime.date(2023, 9, 29), datetime.date(2023, 12, 29)]
    """
    if type(date) != datetime.date:
        date = date.date()
    
    # next 3 days for daily option expiries
    dailies = [date + relativedelta(days=+1), date + relativedelta(days=+2), date + relativedelta(days=+3)]
    
    # next 3 fridays for weekly option expiries
    weeklies = [date + relativedelta(weeks=0, weekday=FR(0)), \
                 date + relativedelta(weeks=+1, weekday=FR(0)), \
                 date + relativedelta(weeks=+2, weekday=FR(0))]
    
    # next 3 last month fridays for monthly option expiries
    monthlies = [date + relativedelta(months=0, day=31,weekday=FR(-1)), \
                 date + relativedelta(months=+1, day=31,weekday=FR(-1)), \
                 date + relativedelta(months=+2, day=31,weekday=FR(-1))]
    
    # next 4 quarters last friday of the month
    quarter_months = list(rrule.rrule(
        freq=rrule.MONTHLY,
        count=4,
        bymonth=(3, 6, 9, 12),
        bysetpos=-1,
        dtstart=date + relativedelta(months=1)))
    quaterlies = [m.date() + relativedelta(day=31, weekday=FR(-1)) for m in quarter_months]
    
    return sorted(set([*dailies, *weeklies, *monthlies, *quaterlies]), key=lambda x: x)

def build_future_expiries(date):

    date = date.date()
    
    # next 2 fridays for weekly expiries
    weeklies = [date + relativedelta(weeks=0, weekday=FR(0)), \
                 date + relativedelta(weeks=+1, weekday=FR(0))]
    
    # next 2 last month fridays for monthly expiries
    monthlies = [date + relativedelta(months=0, day=31,weekday=FR(-1)), \
                 date + relativedelta(months=+1, day=31,weekday=FR(-1))]
    
    # next 4 quarters last friday of the month
    quarter_months = list(rrule.rrule(
        freq=rrule.MONTHLY,
        count=4,
        bymonth=(3, 6, 9, 12),
        bysetpos=-1,
        dtstart=date + relativedelta(months=1)))
    quaterlies = [m.date() + relativedelta(day=31, weekday=FR(-1)) for m in quarter_months]
    
    return sorted(set([*weeklies, *monthlies, *quaterlies]), key=lambda x: x)


def date_to_timestamp(date):
    """
    Convert a datetime.date object to a Unix timestamp.
    """
    epoch = datetime.utcfromtimestamp(0).date()
    delta = date - epoch
    return delta.total_seconds()

def read_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)
    

def get_number_of_timeframes_in_one_day(timeframe):
    """
    Calculates the number of a specific timeframe in one day.

    Args:
        timeframe (str): The timeframe to calculate (e.g., '5m', '15m', '2h', '1d').

    Returns:
        int: The number of the specified timeframes in one day.
    """
    # Dictionary mapping timeframes to their respective minutes
    timeframe_minutes = {
        'm': 1,     # minutes
        'h': 60,    # hours
        'd': 1440   # days
    }

    # Extract the numeric value and timeframe unit from the input
    numeric_value = int(timeframe[:-1])
    timeframe_unit = timeframe[-1]

    # Calculate the number of timeframes in one day
    minutes_in_one_day = 24 * 60
    minutes_per_timeframe = numeric_value * timeframe_minutes[timeframe_unit]
    timeframes_in_one_day = minutes_in_one_day // minutes_per_timeframe

    return timeframes_in_one_day

def add_tenor(date, tenor):
    unit = tenor[-1].upper()
    value = int(tenor[:-1])
    
    if unit == 'D':
        return date + relativedelta(days=value)
    elif unit == 'M':
        return date + relativedelta(months=value)
    elif unit == 'Y':
        return date + relativedelta(years=value)
    else:
        raise ValueError("Invalid tenor notation.")