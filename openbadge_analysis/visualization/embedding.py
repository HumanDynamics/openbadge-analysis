import numpy as np
import pandas as pd
from sklearn.manifold import MDS


def rssi_to_distance(rssi):
    """Computes a distance in meters from an RSSI value.

    Parameters
    ----------
    rssi : number or array
        The RSSI value, or an array of RSSI values.
    
    Returns
    -------
    number or array :
        The distance, or an array of distances, measured in meters.
    """
    # TODO: find the *actual* values for P and n
    P = -60
    n = 2.5
    return np.power(10, (P - rssi)/(10*n))


def members_2d_embedding(m2m, random_state=0):
    """Computes a 2D embedding of the members using aggregated distances.
    
    Parameters
    ----------
    m2m : pandas.DataFrame
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

