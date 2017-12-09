import pandas as pd
import json


def member_to_badge_proximity(fileobject, time_bins_size='1min', tz='US/Eastern'):
    """Creates a member-to-badge proximity DataFrame from a cleaned-up proximity data file.
    
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
                    float(distance['count']),
                )

    df = pd.DataFrame(
            readfile(fileobject),
            columns=('timestamp', 'member', 'observed_id', 'rssi', 'count')
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
    # * weighted_mean - take the average RSSI weighted by the counts, and the sum of the counts
    # * max - take the max value
    df['rssi_weighted'] = df['count'] * df['rssi']
    agg_f = {'rssi': ['max'], 'rssi_weighted': ['sum'], 'count': ['sum']}
    df = df.groupby(level=df.index.names).agg(agg_f)
    df['rssi_weighted'] /= df['count']

    # rename columnes
    df.columns = ['count_sum', 'rssi_max', 'rssi_weighted_mean']
    df['rssi'] = df['rssi_weighted_mean']  # for backward compatibility

    # Select only the fields 'rssi' and 'count'
    return df[['rssi', 'rssi_max', 'rssi_weighted_mean', 'count_sum']]


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

    return df[['rssi', 'count']]

