import pandas as pd
import numpy as np
from ..core import load_proximity_chunks_as_json_objects


def load_proximity_data_from_logs(logs, log_version=None, time_bins_size='1min', tz='US/Eastern'):
    """Creates a resampled proximity DataFrame from a set of proximity logs.
    
    Parameters
    ----------
    logs : list of str
        The paths to the proximity logs.
    
    log_version : str or None
        The version of the logs, in case the files are missing a header.
    
    time_bins_size : str
        The size, in units of time, of the time bins used for the resampling.
        Defaults to '1min', the resolution of the badges.
    
    Returns
    -------
    pandas.DataFrame :
        A pandas DataFrame with each row containing a single proximity record.
    """

    prox = pd.DataFrame(columns=(
        'datetime', 'member', 'voltage', 'observed_id', 'rssi', 'count'
    ))

    prox['datetime'] = pd.to_datetime(prox['datetime'], unit='s', utc=True) \
                       .dt.tz_localize('UTC').dt.tz_convert(tz)
    
    # Load proximity chunks
    # A chunk contains a set of observations by a given badge at a given timestamp
    for filename in logs:
        chunks = []
        with open(filename, 'r') as f:
            chunks.extend(load_proximity_chunks_as_json_objects(f, log_version=log_version))
        
        proximity_data = []
        for chunk in chunks:
            # Iterate over each observation in the chunk
            for (mid, distance) in chunk['rssi_distances'].items():
                proximity_data.append((
                    chunk['timestamp'],
                    chunk['member'],
                    chunk['voltage'],
                    int(mid),  # The id of the observed badge
                    distance['rssi'],
                    distance['count'],
                ))

        df = pd.DataFrame(proximity_data, columns=(
            'timestamp', 'member', 'voltage', 'observed_id', 'rssi', 'count'
        ))
        del proximity_data

        # Convert timestamp to datetime for convenience, and localize to UTC
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
                           .dt.tz_localize('UTC').dt.tz_convert(tz)
        del df['timestamp']

        prox = prox.append(df)
        del df

    # Group per time bins, member and observed_id,
    # and take the first value, arbitrarily
    prox = prox.groupby([
        pd.TimeGrouper(time_bins_size, key='datetime'),
        'member',
        'observed_id'
    ]).first()
#    ]).apply(
#        lambda df: df.loc[df['rssi'].idxmax()]
#    )
    
    # Sort the data
    prox.sort_index(inplace=True)

    return prox


def _load_proximity_data_from_logs(logs, log_version=None, time_bins_size='1min'):
    """Creates a resampled proximity DataFrame from a set of proximity logs.
    
    Parameters
    ----------
    logs : list of str
        The paths to the proximity logs.
    
    log_version : str or None
        The version of the logs, in case the files are missing a header.
    
    time_bins_size : str
        The size, in units of time, of the time bins used for the resampling.
        Defaults to '1min', the resolution of the badges.
    
    Returns
    -------
    pandas.DataFrame :
        A pandas DataFrame with each row containing a single proximity record.
    """
    
    # Load proximity chunks
    # A chunk contains a set of observations by a given badge at a given timestamp
    proximity_chunks = []
    for filename in logs:
        with open(filename, 'r') as f:
            proximity_chunks.extend(load_proximity_chunks_as_json_objects(f, log_version=log_version))
    
    # Format the proximity chunks into proximity data
    # Basically, we split each chunk into a unique row per observed badge
    proximity_data = []
    for chunk in proximity_chunks:
        # Iterate over each observation in the chunk
        for (mid, distance) in chunk['rssi_distances'].items():
            proximity_data.append((
                chunk['timestamp'],
                chunk['member'],
                chunk['voltage'],
                int(mid),  # The id of the observed badge
                distance['rssi'],
                distance['count'],
            ))
    
    # Create a DataFrame from the proximity data
    df = pd.DataFrame(proximity_data, columns=(
        'timestamp', 'member', 'voltage', 'observed_id', 'rssi', 'count'
    ))
    
    # Convert timestamp to datetime for convenience, and localize to UTC
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
                       .dt.tz_localize('UTC')
    del df['timestamp']
    
    # Group per time bins, member and observed_id,
    # and take the first value, arbitrarily
    df = df.groupby([
        pd.TimeGrouper('1min', key='datetime'),
        'member',
        'observed_id'
    ]).first()
#    ]).apply(
#        lambda df: df.loc[df['rssi'].idxmax()]
#    )
    
    # Sort the data
    df.sort_index(inplace=True)
    
    return df


def member_to_member_proximity(proximity_data, members_badges):
    """Creates a member-to-member proximity data DataFrame from the raw proximity data.
    
    Parameters
    ----------
    proximity_data : pandas.DataFrame
        The raw proximity data, as returned by `load_proximity_data_from_logs`.
    
    members_badges : pandas.DataFrame
        The badges used by each member, indexed by datetime and badge id, as returned by
        `load_member_badges_from_logs`.
    
    Returns
    -------
    pandas.DataFrame :
        The member-to-member proximity data.
    """
    
    df = proximity_data.copy().reset_index()

    # Join the member names using their badge ids
    df = df.join(members_badges, on=['datetime', 'observed_id'], lsuffix='1', rsuffix='2')

    # Filter out the beacons (i.e. those ids that did not have a mapping)
    df.dropna(axis=0, subset=['member2'], inplace=True)

    # Set the index and sort it
    df.set_index(['datetime', 'member1', 'member2'], inplace=True)
    df.sort_index(inplace=True)

    # Remove duplicate indexes, keeping the first (arbitrarily)
    df = df[~df.index.duplicated(keep='first')]
    
    # Reorder the index such that 'member1' is always lexicographically smaller than 'member2'
    df.index = df.index.map(lambda ix: (ix[0], min(ix[1], ix[2]), max(ix[1], ix[2])))
    df.index.names = ['datetime', 'member1', 'member2']

    # For cases where we had proximity data coming from both sides,
    # we take the average RSSI weighted by the counts, and the sum of the counts
    df['rssi'] *= df['count']
    df = df.groupby(level=df.index.names).sum()
    df['rssi'] /= df['count']

    # Select only the fields 'rssi' and 'count'
    return df[['rssi', 'count']]


def member_to_beacon_proximity(proximity_data, beacons):
    """Creates a member-to-beacon proximity data DataFrame from the raw
    proximity data.
    
    Parameters
    ----------
    proximity_data : pandas.DataFrame
        The raw proximity data, as returned by
        `load_proximity_data_from_logs`.
    
    beacons : list of str
        A list of beacon ids.
    
    Returns
    -------
    pandas.DataFrame :
        The member-to-member proximity data.
    """
    
    df = proximity_data.copy()
    
    # Remove voltage
    del df['voltage']
    
    # Rename 'observed_id' to 'beacon'
    df = df.rename_axis(['datetime', 'member', 'beacon'])
    
    # Filter out ids that are not in `beacons`
    return df.loc[pd.IndexSlice[:, :, beacons],:]

