import pandas as pd
import json
import io
from ..core import mac_address_to_id

def _id_to_member_mapping_fill_gaps(idmap, time_bins_size='1min'):
    """ Fill gaps in a idmap
    Parameters
    ----------
    idmap : id mapping object

    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.

    Returns
    -------
    pd.DataFrame :
        idmap, after filling gaps.
    """
    df = idmap.to_frame().reset_index()
    df.set_index('datetime', inplace=True)
    s = df.groupby(['id'])['member'].resample(time_bins_size).fillna(method='ffill')
    s = s.reorder_levels((1,0)).sort_index()
    return s


def legacy_id_to_member_mapping(fileobject, time_bins_size='1min', tz='US/Eastern', fill_gaps=True):
    """Creates a mapping from badge id to member, for each time bin, from proximity data file.
    Depending on the version of the logfile (and it's content), it will either use the member_id
    field to generate the mapping (newer version), or calculate an ID form the MAC address (this
    was the default behavior of the older version of the hubs and badges)
    
    Parameters
    ----------
    fileobject : file or iterable list of str
        The proximity data, as an iterable of JSON strings.
    
    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.
    
    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    fill_gaps : boolean
        If True, the code will ensure that a value exists for every time by by filling the gaps
        with the last seen value

    Returns
    -------
    pd.Series :
        A mapping from badge id to member, indexed by datetime and id.
    """
    
    def readfile(fileobject):
        no_id_warning = False
        for line in fileobject:
            data = json.loads(line)['data']
            member_id = None
            if 'member_id' in data:
                member_id = data['member_id']
            else:
                member_id = mac_address_to_id(data['badge_address'])
                if not no_id_warning:
                    print("Warning - no id provided in data. Calculating id from MAC address")
                no_id_warning = True

            yield (data['timestamp'],
                   member_id,
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

    # Extract series
    s = df.sort_index()['member']

    # Fill in gaps, if requested to do so
    if fill_gaps:
        s = _id_to_member_mapping_fill_gaps(s, time_bins_size=time_bins_size)

    return s


def id_to_member_mapping(mapper, time_bins_size='1min', tz='US/Eastern', fill_gaps=True):
    """Creates a pd.Series mapping member numeric IDs to the string
    member key associated with them. 

    If the 'mapper' provided is a DataFrame, assumes it's metadata and that ID's 
        do not change mapping throughout the project, and proceeds to create a
        Series with only a member index.
    If the 'mapper' provided is a file object, assumes the old version of id_map
        and creates a Series with a datetime and member index.

    Parameters
    ----------
    fileobject : file object
        A file to read to determine the mapping.
    
    members_metadata : pd.DataFrame
        Metadata dataframe, as downloaded from the server, to map IDs to keys.
        
    Returns
    -------
    pd.Series : 
        The ID to member key mapping.
    
    """
    if isinstance(mapper, io.BufferedIOBase) | isinstance(mapper, file):
        idmap = legacy_id_to_member_mapping(mapper, time_bins_size=time_bins_size, tz=tz, fill_gaps=fill_gaps)
        return idmap
    elif isinstance(mapper, pd.DataFrame):
        idmap = {row.member_id: row.member for row in mapper.itertuples()}
        return pd.DataFrame.from_dict(idmap, orient='index')[0].rename('member')
    else:
        raise ValueError("You must provide either a fileobject or metadata dataframe as the mapper.")


def voltages(fileobject, time_bins_size='1min', tz='US/Eastern', skip_errors=False):
    """Creates a DataFrame of voltages, for each member and time bin.
    
    Parameters
    ----------
    fileobject : file or iterable list of str
        The proximity data, as an iterable of JSON strings.
    
    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.
    
    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    skip_errors : boolean
        If set to True, skip errors in the data file

    Returns
    -------
    pd.Series :
        Voltages, indexed by datetime and member.
    """
    
    def readfile(fileobject, skip_errors):
        i = 0
        for line in fileobject:
            i = i + 1
            try:
                data = json.loads(line)['data']

                yield (data['timestamp'],
                       str(data['member']),
                       float(data['voltage']))
            except:
                print("Error in line#:", i, line)
                if skip_errors:
                    continue
                else:
                    raise

    df = pd.DataFrame(readfile(fileobject, skip_errors), columns=['timestamp', 'member', 'voltage'])

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


def sample_counts(fileobject, tz='US/Eastern', keep_type=False, skip_errors=False):
    """Creates a DataFrame of sample counts, for each member and raw record

    Parameters
    ----------
    fileobject : file or iterable list of str
        The proximity or audio data, as an iterable of JSON strings.

    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    keep_type : boolean
        If set to True, the type of the record will be returned as well

    skip_errors : boolean
        If set to True, skip errors in the data file

    Returns
    -------
    pd.Series :
        Counts, indexed by datetime, type and member.
    """

    def readfile(fileobject, skip_errors=False):
        i = 0
        for line in fileobject:
            i = i + 1
            try:
                raw_data = json.loads(line)
                data = raw_data['data']
                type = raw_data['type']

                if type == 'proximity received':
                    cnt = len(data['rssi_distances'])
                elif type == 'audio received':
                    cnt = len(data['samples'])
                else:
                    cnt = -1

                yield (data['timestamp'],
                       str(type),
                       str(data['member']),
                       int(cnt))
            except:
                print("Error in line#:", i, line)
                if skip_errors:
                    continue
                else:
                    raise

    df = pd.DataFrame(readfile(fileobject, skip_errors), columns=['timestamp' ,'type', 'member',
                                                     'cnt'])

    # Convert the timestamp to a datetime, localized in UTC
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
        .dt.tz_localize('UTC').dt.tz_convert(tz)
    del df['timestamp']

    if keep_type:
        df.set_index(['datetime','type','member'],inplace=True)
    else:
        del df['type']
        df.set_index(['datetime', 'member'], inplace=True)
    df.sort_index(inplace=True)

    return df
