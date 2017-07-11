import pandas as pd
import numpy as np
from ..core import load_proximity_chunks_as_json_objects


def load_proximity_data_from_logs(logs, log_version=None):
    """Creates a proximity data DataFrame from a set of proxmity logs.
    
    Parameters
    ----------
    logs : list of str
        The paths to the proximity logs.
    
    log_version : str or None
        The version of the logs, in case the files are missing a header.
    
    Returns
    -------
    DataFrame :
        A pandas DataFrame with each row containing a single proximity record."""
    
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
                chunk['badge_address'],
                chunk['voltage'],
                mid,  # The id of the observed badge
                distance['rssi'],
                distance['count'],
            ))
    
    # Create a DataFrame from the proximity data
    df = pd.DataFrame(proximity_data, columns=(
        'timestamp', 'member', 'badge_address', 'voltage', 'observed_id', 'rssi', 'count'
    ))
    # Convert timestamp to datetime for convenience
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    del df['timestamp']
    
    df.sort_values(['datetime', 'member', 'observed_id'], inplace=True)
    df = df.groupby(['datetime', 'member', 'observed_id']).mean()  # In case we have multiple observations
    
    return df


def member_to_member_proximity(proximity_data, id_mapping):
    """Creates a member-to-member proximity data DataFrame from the raw proximity data.
    
    Parameters
    ----------
    proximity_data : DataFrame
        The raw proximity data, as returned by `load_proximity_data_from_logs`.
    
    id_mapping : dict
        A mapping from ids to member.
    
    Returns
    -------
    DataFrame :
        The member-to-member proximity data."""
    
    df = proximity_data.reset_index()
    
    # Remove voltage
    del df['voltage']
    
    # Rename `member` to `member1`
    df['member1'] = df['member']
    del df['member']
    
    # Convert observed id to member name
    df['member2'] = df.apply(lambda row: id_mapping.get(row['observed_id'], None), axis=1)
    del df['observed_id']
    
    # Filter out the beacons (i.e. those ids that did not have a mapping)
    df.dropna(axis=0, subset=['member2'], inplace=True)
    
    # Sort the values
    df.sort_values(['datetime', 'member1', 'member2'], inplace=True)
    
    df = df.groupby(['datetime', 'member1', 'member2']).mean()  # In case we have multiple observations
    
    return df


def dyadic_proximity(m2m, time_bin='1min'):
	"""Extracts a single RSSI measure per dyad and time bin, from the
	member-to-member proximity data.
	
	In the case where two or more values exist for a single time bin (i.e.
	the two badges "saw" each other during that time bin), the largest value
	is selected, as advised by Sekara, V. & Lehmann, S. The strength of
	friendship ties in proximity sensor data. PLoS One 9, e100915 (2014).
	
	Parameter
	---------
	m2m : DataFrame
		Member-to-member proximity data, as returned by
		`member_to_member_proximity`.
	
	time_bin : str
		The size of the time bins to use, e.g. '1min', '5min', etc.  Defaults
		to '1min'.
	
	Returns
	-------
	DataFrame :
		An RSSI value for each dyad (order-less pair of members), for each
		time bin.
	"""
	
	df = m2m.copy().reset_index()
	
	# Build a list of dyads from the columns 'member1' and 'member2'
	# Dyads are represented as `frozenset`, which are nothing more than
	# constant (and hence, python-hashable) sets
	df['dyad'] = [frozenset(tpl) if len(set(tpl)) == 2 else np.nan for tpl in zip(df['member1'], df['member2'])]
	
	# Drop those dyads that have a single value
	df.dropna(inplace=True)
	
	df = df[['dyad', 'datetime', 'rssi']]
	
	# Group by dyad, resample and select maximum value
	df = df.set_index('datetime').groupby(['dyad', pd.TimeGrouper(time_bin)]).max()

	return df

