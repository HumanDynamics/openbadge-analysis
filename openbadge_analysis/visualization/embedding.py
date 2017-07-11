import numpy as np
import pandas as pd
from sklearn.manifold import MDS, TSNE


# TODO: make this more general, give the possibility to specify the column and whether it is
# a similarity or a distance
def members_2d_embedding(m2m, self_sym_factor=1.2):
    """Computes a 2D embedding of the members using aggregated count values.
    
    Parameters
    ----------
    m2m : DataFrame
        Count values aggregated over pairs of members.  This can be obtained with `groupby` over the
        member-to-member proximity data.
    
    self_sym_factor : float
        The self-symilarity factor.  See the source for more information.
    
    Returns
    -------
    DataFrame :
        The (x, y) coordinates of each member.
    """
    
    # Pivot the data to get a table of count value for each pair of member
    count_table = m2m.reset_index().pivot(index='member1', columns='member2', values='count')
    
    # Restrict the indices and columns to only those members that appear in both
    # This ensures that we won't have a member who only appears as a column or as a row
    members = list(set(count_table.columns).intersection(count_table.index.values))
    count_table = count_table.loc[members]
    count_table = count_table[members]
    
    # No data means count = 0
    count_table.fillna(0.0, inplace=True)
    
    # Sort the column names and index values, such that a given member occupies the same column and row index
    # This is very important, as the next step is to drop the index/column names altogether for the MDS
    count_table = count_table.sort_index(axis=0).sort_index(axis=1)
    
    
    # Get the matrix from the table
    # This is not a similarity matrix, because the diagonal is zero
    M = count_table.as_matrix()
    M = M + M.transpose()  # Make it symmetric
    
    # This is a somewhat arbitrary value for the self-similarity of a member
    # It is defined as the maximum similarity found in the matrix, multiplied by an arbitrary factor
    # The greater this factor, the greater the minimum distance between two very similar (close) members
    self_sym = M.max().max()*self_sym_factor
    
    # Convert the matrix to a similarity matrix, by setting the diagonal to `self_sym`
    M = M + np.eye(*M.shape)*self_sym
    
    # Convert it to a distance matrix
    M = self_sym - M
    
    
    # Using the multidimensional scaling model from sklearn,
    # we produce a 2d embedding of the data from the distance matrix
    mds = MDS(metric=True, dissimilarity='precomputed', random_state=0)
    positions = mds.fit_transform(M)
    
    # Return a dataframe of 2d coordinates associated to each member
    return pd.DataFrame.from_records(
        positions,
        index=count_table.index.values,
        columns=('x', 'y')
    ).rename_axis('member')
