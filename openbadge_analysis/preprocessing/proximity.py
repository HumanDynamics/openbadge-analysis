import pandas as pd
import json


def member_to_badge_proximity_smooth(m2badge, window_size = '5min',
                                      min_samples = 1):
    """ Smooths the given object using 1-D median filter
    Parameters
    ----------
    m2badge : Member to badge object

    window_size : str
        The size of the window used for smoothing.  Defaults to '5min'.

    min_samples : int
        Minimum number of samples required for smoothing

    Returns
    -------
    pd.DataFrame :
        The member-to-badge proximity data, after smoothing.
    """
    df = m2badge.copy().reset_index()
    df = df.sort_values(by=['member', 'observed_id', 'datetime'])
    df.set_index('datetime', inplace=True)

    df2 = df.groupby(['member', 'observed_id'])[['rssi']] \
        .rolling(window=window_size, min_periods=min_samples) \
        .median()

    df2['rssi_std']\
        = df.groupby(['member', 'observed_id'])[['rssi']] \
        .rolling(window=window_size, min_periods=min_samples) \
        .std()

    df2 = df2.reorder_levels(['datetime', 'member', 'observed_id'], axis=0)\
        .dropna().sort_index()
    return df2

def member_to_badge_proximity_fill_gaps(m2badge, time_bins_size='1min',
                                        max_gap_size = 2):
    """ Fill gaps in a given member to badge object
    Parameters
    ----------
    m2badge : Member to badge object

    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.

    max_gap_size : int
         this is the maximum number of consecutive NaN values to forward/backward fill

    Returns
    -------
    pd.DataFrame :
        The member-to-badge proximity data, after filling gaps.
    """

    df = m2badge.copy().reset_index()
    df = df.sort_values(by=['member', 'observed_id', 'datetime'])
    df.set_index('datetime', inplace=True)

    df = df.groupby(['member', 'observed_id'])[['rssi', 'rssi_std']] \
        .resample(time_bins_size) \
        .fillna(method='ffill', limit=max_gap_size)

    df = df.reorder_levels(['datetime', 'member', 'observed_id'], axis=0)\
        .dropna().sort_index()
    return df


def member_to_badge_proximity(fileobject, time_bins_size='1min', tz='US/Eastern'):
    """Creates a member-to-badge proximity DataFrame from a proximity data file.
    
    Parameters
    ----------
    fileobject : file or iterable list of str
        The proximity data, as an iterable of JSON strings.
    
    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.
    
    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.
    
    Returns
    -------
    pd.DataFrame :
        The member-to-badge proximity data.
    """
    
    def readfile(fileobject):
        for line in fileobject:
            data = json.loads(line)['data']

            for (observed_id, distance) in data['rssi_distances'].items():
                yield (
                    data['timestamp'],
                    str(data['member']),
                    int(observed_id),
                    float(distance['rssi']),
                    #float(distance['count']), # removing the count since it's
                                               # confusing when you use
                                               #smoothing
                )

    df = pd.DataFrame(
            readfile(fileobject),
            columns=('timestamp', 'member', 'observed_id', 'rssi')
    )

    # Convert timestamp to datetime for convenience, and localize to UTC
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
            .dt.tz_localize('UTC').dt.tz_convert(tz)
    del df['timestamp']

    # Group per time bins, member and observed_id,
    # and take the first value, arbitrarily
    df = df.groupby([
        pd.TimeGrouper(time_bins_size, key='datetime'),
        'member',
        'observed_id'
    ]).first()

    # Sort the data
    df.sort_index(inplace=True)

    return df


def member_to_member_proximity(m2badge, id2m):
    """Creates a member-to-member proximity DataFrame from member-to-badge proximity data.
    
    Parameters
    ----------
    m2badge : pd.DataFrame
        The member-to-badge proximity data, as returned by `member_to_badge_proximity`.
    
    id2m : pd.Series
        The badge IDs used by each member, indexed by datetime and badge id, as returned by
        `id_to_member_mapping`.
    
    Returns
    -------
    pd.DataFrame :
        The member-to-member proximity data.
    """
    
    df = m2badge.copy().reset_index()

    # Join the member names using their badge ids
    df = df.join(id2m, on=['datetime', 'observed_id'], lsuffix='1', rsuffix='2')

    # Filter out the beacons (i.e. those ids that did not have a mapping)
    df.dropna(axis=0, subset=['member2'], inplace=True)

    # Reset the members type to their original type
    # This is done because pandas likes to convert ints to floats when there are
    # missing values
    df['member2'] = df['member2'].astype(id2m.dtype)

    # Set the index and sort it
    df.set_index(['datetime', 'member1', 'member2'], inplace=True)
    df.sort_index(inplace=True)

    # Remove duplicate indexes, keeping the first (arbitrarily)
    df = df[~df.index.duplicated(keep='first')]

    # If the dataframe is empty after the join, we can (and should) stop
    # here
    if len(df) == 0:
        return df

    # Reorder the index such that 'member1' is always lexicographically smaller than 'member2'
    df.index = df.index.map(lambda ix: (ix[0], min(ix[1], ix[2]), max(ix[1], ix[2])))
    df.index.names = ['datetime', 'member1', 'member2']

    # For cases where we had proximity data coming from both sides,
    # we calculate two types of rssi:
    # * mean - take the average RSSI
    # * max - take the max value
    agg_f = {'rssi': ['max','mean']}
    df = df.groupby(level=df.index.names).agg(agg_f)

    # rename columnes
    df.columns = ['rssi_max', 'rssi_mean']
    df['rssi'] = df['rssi_mean']  # for backward compatibility

    # Select only the fields we need
    return df[['rssi', 'rssi_max', 'rssi_mean']]


def _member_to_beacon_proximity(m2badge, beacons):
    """Creates a member-to-beacon proximity DataFrame from member-to-badge proximity data.
    
    Parameters
    ----------
    m2badge : pd.DataFrame
        The member-to-badge proximity data, as returned by `member_to_badge_proximity`.
    
    beacons : list of str
        A list of beacon ids.
    
    Returns
    -------
    pd.DataFrame :
        The member-to-member proximity data.
    """
    
    df = m2badge.copy()
    
    # Rename 'observed_id' to 'beacon'
    df = df.rename_axis(['datetime', 'member', 'beacon'])
    
    # Filter out ids that are not in `beacons`
    return df.loc[pd.IndexSlice[:, :, beacons],:]


def member_to_beacon_proximity(m2badge, id2b):
    """Creates a member-to-beacon proximity DataFrame from member-to-badge proximity data.
    
    Parameters
    ----------
    m2badge : pd.DataFrame
        The member-to-badge proximity data, as returned by `member_to_badge_proximity`.
    
    id2b : pd.Series
        A mapping from badge ID to beacon name.  Index must be ID, and series name must be 'beacon'.
    
    Returns
    -------
    pd.DataFrame :
        The member-to-member proximity data.
    """
    
    df = m2badge.copy().reset_index()

    # Join the beacon names using their badge ids
    df = df.join(id2b, on='observed_id') 

    # Filter out the members (i.e. those ids that did not have a mapping)
    df.dropna(axis=0, subset=['beacon'], inplace=True)

    # Reset the beacons type to their original type
    # This is done because pandas likes to convert ints to floats when there are
    # missing values
    df['beacon'] = df['beacon'].astype(id2b.dtype)

    # Set the index and sort it
    df.set_index(['datetime', 'member', 'beacon'], inplace=True)
    df.sort_index(inplace=True)

    # Remove duplicate indexes, keeping the first (arbitrarily)
    df = df[~df.index.duplicated(keep='first')]

    return df[['rssi', 'rssi_std']]

