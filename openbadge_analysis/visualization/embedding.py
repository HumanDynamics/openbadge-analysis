import numpy as np
import pandas as pd
from sklearn.manifold import MDS
from .mds_with_anchors import smacof


def rssi_to_distance(rssi, P=-61.5, n=2.6):
    """Computes a distance in meters from an RSSI value.

    Parameters
    ----------
    rssi : number or array
        The RSSI value, or an array of RSSI values.

    P : float
        The RSSI value for 1 meter.  Defaults to -61.5.

    n : float
        The "scaling factor" used when converting RSSI to meters.  Defaults to 2.6.
    
    Returns
    -------
    number or array :
        The distance, or an array of distances, measured in meters.
    """
    # NOTE for the paper I used -60 and 2.4
    return np.power(10, (P - rssi)/(10*n))


def coords_to_distance(coords):
    """Transforms a list of coordinates into a list of distances between each
    point of these coordinates.
    
    Parameters
    ----------
    coords : pd.DataFrame
        The coordinates, for each point.  Must have a single index, and columns
        'x' and 'y'.
    
    Returns
    -------
    pd.Series :
        The distances, indexed by the index name of `coords` suffixed with '1'
        and '2'.
    """
    
    index_name = coords.index.names[0]
    
    coords = coords.copy()

    # Make the scalar product of `coords` with itself
    coords['_tmpkey'] = 1
    distances = pd.merge(coords, coords, on='_tmpkey', suffixes=('1', '2')).drop('_tmpkey', axis=1)
    distances.index = pd.MultiIndex.from_product((coords.index, coords.index))
    distances.index.names = [index_name + '1', index_name + '2']
    coords.drop('_tmpkey', axis=1, inplace=True)

    del coords
    
    # Compute the Euclidean distance for each pair of points
    distances['distance'] = distances.apply(
        lambda df: np.sqrt((df['x1'] - df['x2'])**2 + (df['y1'] - df['y2'])**2),
        axis=1
    )
    
    return distances['distance']


def _members_2d_embedding(m2m, random_state=0):
    """Computes a 2D embedding of the members using aggregated distances.
    
    Parameters
    ----------
    m2m : pd.DataFrame
        Member-to-member proximity data, indexed by 'datetime', 'member1', and 'member2'.

    random_state : int
        The seed used by sklearn when computing the embedding.
    
    Returns
    -------
    DataFrame :
        The (x, y) coordinates of each member.
    """

    # Compute the distances using the RSSI values
    distances = rssi_to_distance(m2m['rssi'])

    # Add redundant data so the pivoted dataframe will be symmetrical
    distances = distances.append(distances.reorder_levels(['datetime', 'member2', 'member1']))

    # Average distances over time
    distances = distances.groupby(['member1', 'member2']).mean()
    
    # Pivot the dataframe and fill the missing values with the maximum distance
    # This choice for filling the NAs is completely arbitrary, and was not based on
    # any research
    # TODO: it would probably be good to investigate this, and to try and see
    # whether there is a better way to do things
    pivoted = distances.unstack().fillna(distances.max())

    # Make sure that the indices and columns are the same and in the same order, so we can use
    # `as_matrix()` to pass the distances to sklearn
    members = list(set(pivoted.columns).intersection(pivoted.index.values))
    pivoted = pivoted.loc[members]
    pivoted = pivoted[members]
    pivoted = pivoted.sort_index(axis=0).sort_index(axis=1)

    # Convert the pivoted dataframe to a matrix
    M = pivoted.as_matrix()
    
    # Fill the diagonal with 0s
    np.fill_diagonal(M, 0.0)

    # Compute the 2D embedding using multidimensional scaling
    mds = MDS(dissimilarity='precomputed', random_state=random_state)
    positions = mds.fit_transform(M)

    # Return a dataframe of 2d coordinates associated to each member
    return pd.DataFrame.from_records(
        positions,
        index=pivoted.index.values,
        columns=('x', 'y')
    ).rename_axis('member')


def members_2d_embedding(m2m, prev=None, P=-61.5, n=2.6, random_state=0):
    """Computes a 2D embedding of the members through multidimensional scaling.
    
    Parameters
    ----------
    m2m : pandas.DataFrame
        Member-to-member proximity data, indexed by 'datetime', 'member1', and 'member2'.
        
    prev : None or pd.DataFrame
        If not None, then the positions of the members, as returned by this function.  This
        can be used to initialized the MDS with the values from a previously computed MDS
        (e.g. to create a time series).  Defaults to None.
    
    P : float
    n : float
        The parameters for `rssi_to_distance`.

    random_state : None, int or np.random.RandomState
        The seed/RNG used by sklearn when computing the embedding.  Defaults to None.
    
    Returns
    -------
    DataFrame :
        The (x, y) coordinates of each member.
    """
    
    # Initialize the RNG
    if type(random_state) != np.random.RandomState:
        random_state = np.random.RandomState(random_state)
    
    # The list of members
    members = sorted(list(set(list(m2m.reset_index()['member1']) + list(m2m.reset_index()['member2']))))
    
    # Member-to-member
    df = rssi_to_distance(m2m['rssi'], P=P, n=n).groupby(['member1', 'member2']).mean()
    df = df.append(df.reorder_levels([1, 0]))
    distances = df
    
    # Diagonal
    distances = distances.append(pd.Series(np.zeros(len(members)),
                                           index=pd.MultiIndex.from_tuples(zip(members, members))))
    
    # Remove duplicates, in case there are any (there shouldn't be)
    distances = distances[~distances.index.duplicated(keep='first')]
    
    # Distance matrix
    dist_matrix = distances.unstack()
    
    # This is to deal with a "bug/feature" in pandas
    # Using .loc on a DF that has non-int indexes will call .iloc internally, which is not what we want
    # Converting the indexes to a string fixes the problem
    dist_matrix.columns = dist_matrix.columns.astype(str)
    dist_matrix.index = dist_matrix.index.astype(str)
    
    # Convert into a numpy matrix, with the rows/columns containing
    # the members first and the beacons second
    D = dist_matrix.loc[map(str, members), map(str, members)].as_matrix()
    
    # The weight matrix is set to 0. for missing values, and 1. everywhere else
    W = 1 - np.isnan(D)

    # Missing values in `D`
    D[np.isnan(D)] = 0.0  # Arbitrary value; those distances are ignored anyway
    
    init = None
    # If previous values were given for the members positions
    if prev is not None:
        # Select those that match our list of members
        prev = prev.loc[members]
        # Fill the NA's with random values
        prev = prev.fillna(pd.DataFrame(random_state.randn(*np.isnan(previous).shape),
                                        index=members, columns=['x', 'y']))
        init = prev.as_matrix()
        
    # Compute the embedding using the SMACOF algorithm
    positions = smacof(D, weights=W, init=init, random_state=random_state)
    
    # Return a dataframe of 2d coordinates associated to each member
    return pd.DataFrame(
        positions,
        index=members,
        columns=['x', 'y']
    ).rename_axis('member')


def members_2d_embedding_with_beacons(m2m, m2b, beacons_position, prev=None, m2m_weight=1., m2b_weight=1.,
                                      P=-61.5, n=2.6, random_state=None):
    """Computes a 2D embedding of the members using aggregated distances and the beacons
    data.
    
    Parameters
    ----------
    m2m : pd.DataFrame
        Member-to-member proximity data, indexed by 'datetime', 'member1', and 'member2',
        with column 'rssi'.
        
    m2b : pd.DataFrame
        Member-to-beacon proximity data, indexed by 'datetime', 'member', and 'beacon',
        with column 'rssi'.
        
    beacons_position : pd.DataFrame
        The position of each beacon, indexed by 'beacon', with columns 'x' and 'y'.
        
    prev : None or pd.DataFrame
        If not None, then the positions of the members, as returned by this function.  This
        can be used to initialized the MDS with the values from a previously computed MDS
        (e.g. to create a time series).  Defaults to None.
    
    m2m_weight : float
    m2b_weight : float
        The weight to give to member-to-member (resp. member-to-beacon) distances in the
        SMACOF algorithm.  A higher weight means that the algorithm will try to have those
        distances respected better.  Defaults to 1. for both.
    
    P : float
    n : float
        The parameters for `rssi_to_distance`.  Defaults to their default value in
        `rssi_to_distance`.

    random_state : None, int or np.random.RandomState
        The seed/RNG used by sklearn when computing the embedding.  Defaults to None.
    
    Returns
    -------
    DataFrame :
        The (x, y) coordinates of each member.
    """
    
    # Initialize the RNG
    if type(random_state) != np.random.RandomState:
        random_state = np.random.RandomState(random_state)
    
    # The list of members
    members = sorted(list(set(list(m2m.reset_index()['member1']) + list(m2m.reset_index()['member2']))))
    # The list of beacons
    beacons = sorted(list(set(m2b.reset_index()['beacon'])))

    # Beacon-to-beacon
    distances = coords_to_distance(beacons_position)
    
    # Member-to-member
    df = rssi_to_distance(m2m['rssi'], P=P, n=n).groupby(['member1', 'member2']).mean()
    df = df.append(df.reorder_levels([1, 0]))
    distances = distances.append(df)
    
    # Member-to-beacon
    df = rssi_to_distance(m2b['rssi'], P=P, n=n).groupby(['member', 'beacon']).mean()
    df = df.append(df.reorder_levels([1, 0]))
    distances = distances.append(df)
    
    # Diagonal
    distances = distances.append(pd.Series(np.zeros(len(members)), 
                       index=pd.MultiIndex.from_tuples(zip(members, members))))
    
    # Remove duplicates, in case there are any (there shouldn't be)
    distances = distances[~distances.index.duplicated(keep='first')]
    
    # Distance matrix
    dist_matrix = distances.unstack()
    
    # This is to deal with a "bug/feature" in pd
    # Using .loc on a DF that has non-int indexes will call .iloc internally, which is not what we want
    # Converting the indexes to a string fixes the problem
    dist_matrix.columns = dist_matrix.columns.astype(str)
    dist_matrix.index = dist_matrix.index.astype(str)
    
    # Convert into a numpy matrix, with the rows/columns containing
    # the members first and the beacons second
    D = dist_matrix.loc[map(str, members + beacons), map(str, members + beacons)].as_matrix()
    
    # The weight matrix is set to 0. for missing values, and 1. everywhere else
    W = 1 - np.isnan(D)
    
    m = len(members)
    # Custom weight for member-to-member
    W[m:,m:] = m2m_weight * W[m:,m:]
    # Custom weight for member-to-beacon
    W[m:,:m] = m2b_weight * W[m:,:m]
    W[:m,m:] = m2b_weight * W[:m,m:]

    # Missing values in `D`
    D[np.isnan(D)] = 0.0  # Arbitrary value; those distances are ignored anyway
    
    # The position of the anchors
    A = beacons_position.loc[beacons].as_matrix()
    
    init = None
    # If previous values were given for the members positions
    if prev is not None:
        # Select those that match our list of members
        prev = prev.loc[members]
        # Fill the NA's with random values
        prev = prev.fillna(pd.DataFrame(random_state.randn(*np.isnan(previous).shape),
                                        index=members, columns=['x', 'y']))
        init = prev.as_matrix()
        
    # Compute the embedding using the SMACOF algorithm
    positions = smacof(D, weights=W, init=init, anchors=A, random_state=random_state)
    
    # Return a dataframe of 2d coordinates associated to each member
    return pd.DataFrame(
        positions,
        index=members,
        columns=['x', 'y']
    ).rename_axis('member')

