import pandas as pd
import numpy as np
import networkx as nx


"""This module is based on the paper: Sekara, Vedran, Arkadiusz Stopczynski, and Sune Lehmann. "Fundamental structures of dynamic social networks." Proceedings of the national academy of sciences 113.36 (2016): 9977-9982.
"""


def extract_groups(m2m):
    """Extracts a list of groups from a social network varying through time.
    
    Groups are defined as connected components of the social graph at a given
    time bin.
    
    Parameters
    ----------
    m2m : pd.DataFrame
        The social network, for instance, member-to-member bluetooth proximity
        data.  It must have the following columns: 'datetime', 'member1', and
        'member2'.
    
    Returns
    -------
    pd.DataFrame :
        The groups, as a sets of members with datetime.
    """
    groups = m2m.groupby('datetime').apply(
        lambda df:
        pd.Series([frozenset(c) for c in nx.connected_components(nx.from_pandas_dataframe(df.reset_index(), 'member1', 'member2'))])
    )
    groups.name = 'members'
    
    return groups.reset_index()[['datetime', 'members']]


def _set_similarity(s, t):
    """Computes the similarity between two sets, namely, the ratio of intersection
    size to union size."""
    return float(len(s.intersection(t)))/len(s.union(t))


def _group_distance(g, h, gamma):
    """Computes a distance metric between two groups, between 0 and 1.

    This metric depends on the set similarity of the two groups, and the number of
    time bins between the two groups.
    
    Parameters
    ----------
    g : tuple
    h : tuple
        Groups, where the first element is a datetime, and the second is a set.
    
    gamma : float
        Exponential decay, for time differences between groups.
    """
    dt = np.abs(g[0] - h[0]).total_seconds()/60
    return 1. - _set_similarity(g[1], h[1]) * np.exp(-gamma * (dt - 1.))


def gather_groups(groups, distance_threshold=.49, gamma=.08):
    """Gather groups into gatherings.
    
    A gathering is defined as series of groups with similar members, existing
    within a somewhat continuous timeframe.
    
    Parameters
    ----------
    groups : pd.DataFrame
        A list of groups, as returned by `extract_groups`.
    
    distance_threshold : float
        The minimum distance between two groups for them to be joined into a
        single gathering.  Default (recommended) value is 0.49.
    
    gamma : float
        The `gamma` parameter of the group distance.
    
    Returns
    -------
    list(pd.DataFrame) :
        A list of gatherings, where each gathering is a DataFrame containing
        the members of the gathering present at each time bin.
    """
    n = len(groups)

    # Convert the DataFrame to an array for fast indexing
    ga = groups.as_matrix()

    # `dist` is the distance matrix of gatherings, "indexed by groups"
    # In other words, `dist` has a column/row for each group.  For each group,
    # it contains the distance from the gathering of this group, to the gathering
    # of every other group.
    # The distance between two gatherings is the minimum distance between every
    # pair of groups.
    # We start with having as many gatherings as groups, and so `dist` begins as
    # the matrix of distances between groups
    dist = np.zeros((n, n))
    for i in range(n):
        for j in range(i+1, n):
            dist[i, j] = _group_distance(ga[i,:], ga[j,:], gamma=gamma)
    dist += dist.T

    # We set the invariant on `dist` that two groups in the same gathering have a
    # distance of 1.0, so they won't be chosen by `np.argmin`
    np.fill_diagonal(dist, 1.0)

    # Mapping group index -> gathering index
    # It's initialized as `i -> i` for all i
    grp2gth = dict(zip(range(n), range(n)))
    # Mapping gathering index -> groups indices
    # It's initialized as `i -> [i]` for all i
    gth2grp = dict(zip(range(n), [[i] for i in range(n)]))

    # As long as there are at least two gatherings
    while len(gth2grp) > 1:
        # Pick the minimum distance between two gatherings
        i, j = np.unravel_index(dist.argmin(), dist.shape)

        # If the minimum distance is above the threshold, stop the algorithm
        if dist[i,j] > distance_threshold:
            break

        # Index of the first gathering
        gth0 = min(grp2gth[i], grp2gth[j])
        # Index of the second gathering
        gth1 = max(grp2gth[i], grp2gth[j])

        # Change the gathering index of the groups in the second gathering
        for g in gth2grp[gth1]:
            grp2gth[g] = gth0

        # Add the groups of the second gathering to the first
        gth2grp[gth0].extend(gth2grp[gth1])
        # Remove the second gathering
        del gth2grp[gth1]

        # The new distance vector of elements of this new gathering is set as
        # the minimum of the vectors of the two original gatherings
        newdist = np.minimum(dist[i,:], dist[j,:])

        # Set the within-gathering distance to 1.0 (enforce the invariant)
        for g in gth2grp[gth0]:
            newdist[g] = 1.0

        # Update the distance of each element of the new gathering
        for g in gth2grp[gth0]:
            dist[g,:] = newdist  # Column
            dist[:,g] = newdist  # Row

    # Store each gathering in a DataFrame
    gatherings = [groups.iloc[gs].copy().set_index('datetime').sort_index()['members']
                  for gs in gth2grp.itervalues()]

    return gatherings


def _participation_threshold(n):
    """Computes the participation threshold for the gap significancy test.

    The threshold is defined as one standard deviation away from the mean gap in
    a vector of `n` uniformly distributed random variables, taking values between
    0 and 1.  This is equivalent to a beta distribution with parameters 1 and `n`.
    """
    mean = 1./(n + 1)
    std = np.sqrt(1.*n/(n+1)**2/(n+2))

    return mean + std


def _extract_core(gathering):
    """Extracts a core from a given gathering, using the gap significancy test in
    the participation profile."""

    # Extract the list of participants to the gathering for each time bin
    # The result is a DataFrame listing each member participating at each time bin
    df = gathering.apply(lambda x: pd.Series(sorted(list(x)))).stack().rename('member').to_frame()

    # Pivot the dataframe, giving a matrix of member vs time bin, with 1 when
    # a member participated to the gathering at that moment, and 0 otherwise
    df['participates'] = 1.
    df = df.reset_index().set_index(['datetime', 'member'])['participates'].unstack().fillna(0.0)

    # Sum the participations and normalize, to get the percentage of participation
    # for each member
    participations = (df.sum()/len(df.index)).sort_values(ascending=False)

    # Compute the participation gap threshold
    threshold = _participation_threshold(len(df.columns))

    # Compute the gaps in the sorted participation profiles
    gaps = participations - participations.shift(-1)

    # Return the participations index, truncated after the first significant
    # gap in participations
    return list(participations.loc[:(gaps > threshold).idxmax()].index)


def extract_cores(gatherings):
    """Extract the core from each gathering.
    
    Parameters
    ----------
    gatherings : list(pd.DataFrame)
        The gatherings, as returned by `gather_groups`.
    
    Returns
    -------
    pd.DataFrame :
        A list of cores, with columns `start`, `end`, and `members`,
        containing for each core the time it started, the time it ended, and its
        members.
    """
    # Extract core from each gathering of
    cores = [(gathering.index.min(),
              gathering.index.max(),
              frozenset(_extract_core(gathering))) for gathering in gatherings if len(gathering) > 2]

    # Filter out cores with a single individual
    cores = filter(lambda x: len(x[2]) > 1, cores)

    # Store the cores in a DataFrame
    cores = pd.DataFrame(cores, columns=['start', 'end', 'members'])

    return cores

