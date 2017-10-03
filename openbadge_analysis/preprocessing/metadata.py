import pandas as pd
import json
from ..core import mac_address_to_id


def id_to_member_mapping(fileobject, time_bins_size='1min', tz='US/Eastern'):
    """Creates a mapping from badge id to member, for each time bin, from a cleaned-up proximity data file.
    
    Parameters
    ----------
    fileobject : file or iterable list of str
        The cleaned-up proximity data, as an iterable of JSON strings.
    
    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.
    
    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.
    
    Returns
    -------
    pd.Series :
        A mapping from badge id to member, indexed by datetime and id.
    """
    
    def readfile(fileobject):
        for line in fileobject:
            data = json.loads(line)

            yield (data['timestamp'],
                   mac_address_to_id(data['badge_address']),
                   str(data['member']))
    
    df = pd.DataFrame(readfile(fileobject), columns=['timestamp', 'id', 'member'])

    # Convert the timestamp to a datetime, localized in UTC
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
            .dt.tz_localize('UTC').dt.tz_convert(tz)
    del df['timestamp']
    
    # Group by id and resample
    df = df.groupby([
        pd.TimeGrouper(time_bins_size, key='datetime'),
        'id'
    ]).first()
    
    df.sort_index(inplace=True)
    
    return df['member']


def voltages(fileobject, time_bins_size='1min', tz='US/Eastern'):
    """Creates a DataFrame of voltages, for each member and time bin, from a cleaned-up proximity data file.
    
    Parameters
    ----------
    fileobject : file or iterable list of str
        The cleaned-up proximity data, as an iterable of JSON strings.
    
    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.
    
    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.
    
    Returns
    -------
    pd.Series :
        Voltages, indexed by datetime and member.
    """
    
    def readfile(fileobject):
        for line in fileobject:
            data = json.loads(line)

            yield (data['timestamp'],
                   str(data['member']),
                   float(data['voltage']))
    
    df = pd.DataFrame(readfile(fileobject), columns=['timestamp', 'member', 'voltage'])

    # Convert the timestamp to a datetime, localized in UTC
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
                       .dt.tz_localize('UTC').dt.tz_convert(tz)
    del df['timestamp']

    # Group by id and resample
    df = df.groupby([
        pd.TimeGrouper(time_bins_size, key='datetime'),
        'member'
    ]).mean()
    
    df.sort_index(inplace=True)
    
    return df['voltage']

