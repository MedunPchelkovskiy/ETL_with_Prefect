import datetime
import calendar

def datetime_to_timestamp(dt_str: str) -> int:
    """
    Convert a datetime string in 'dd-mm-yyyy HH:MM' format to a Unix timestamp (UTC).
    Example: '01-01-2001 13:45:00' → timestamp
    """
    dt = datetime.datetime.strptime(dt_str, "%d-%m-%Y %H:%M:%S")
    timestamp = calendar.timegm(dt.timetuple())
    return timestamp