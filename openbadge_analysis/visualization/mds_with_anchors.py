import numpy as np

from scipy.spatial import distance


def _gt_weights(W):
    """Computes the weights V for a Guttman transform V X = B(X) Z."""
    V = -W
    V[np.diag_indices(V.shape[0])] = W.sum(axis=1) - W.diagonal()

    return V


def _gt_mapping(D, W, Z):
    """Computes the mapping B(X) for a Guttman transform V X = B(X) Z."""
    # Compute the Euclidean distances between all pairs of points
    Dz = distance.cdist(Z, Z)
    # Fill the diagonal of Dz, because *we don't want a division by zero*
    np.fill_diagonal(Dz, 1e-5)

    B = - W * D / Dz
    np.fill_diagonal(B, 0.0)
    B[np.diag_indices(B.shape[0])] = -np.sum(B, axis=1)

    return B


def _guttman_transform(D, W, Zu, Xa, V):
    """Applies the Guttman transform with anchors.

    See Di Franco, Carmelo, et al. "Multidimensional scaling localization with anchors."
    Autonomous Robot Systems and Competitions (ICARSC), 2017 IEEE International Conference on. IEEE, 2017.
    """
    n, m = Zu.shape[0], Xa.shape[0]
    Z = np.vstack([Zu, Xa])
    V11, V12 = V[0:n, 0:n], V[0:n, n:]
    B = _gt_mapping(D, W, Z)
    B11, B12 = B[0:n, 0:n], B[0:n, n:]

    #return np.linalg.solve(
    #        V11,
    #        np.dot(B11, Zu) + np.dot(B12, Za) - np.dot(V12, Xa)
    #        )

    return np.dot(
            np.linalg.pinv(V11),
            np.dot(B11, Zu) + np.dot(B12, Xa) - np.dot(V12, Xa)
            )


def _stress(D, W, X):
    """Computes the value of the weighted stress function of the MDS."""
    Dx = distance.cdist(X, X)
    S = W * (D - Dx)**2
    return np.triu(S, 1).sum()
    

def _smacof_single(dissimilarities, weights, init=None, anchors=None, n_components=2, maxitr=300, eps=1e-6, random_state=None):
    # Pre-compute the weights of the Guttman transform
    V = _gt_weights(weights)

    if random_state is None:
        random_state = np.random.RandomState()

    # Initial positions are random by default
    if init is None:
        init = random_state.randn(dissimilarities.shape[0]-anchors.shape[0], n_components)
    X = init

    Sprev = _stress(dissimilarities, weights, np.vstack([X, anchors]))  # Stress at previous iteration
    for itr in range(maxitr):
        X = _guttman_transform(dissimilarities, weights, X, anchors, V)

        S = _stress(dissimilarities, weights, np.vstack([X, anchors]))
        if np.abs(S - Sprev) < eps:
            break
        Sprev = S

    return X, Sprev


def smacof(dissimilarities, weights=None, init=None, anchors=None, n_components=2, n_init=8, maxitr=300, eps=1e-6, random_state=None):
    """Executes the SMACOF with anchors algorithm to find a Euclidean embedding of dissimilarities between n samples in a d-dimensional space.
    
    Parameters
    ----------
    dissimilarities : n-by-n matrix
        The distances/dissimilarities between each pair sample, as a two-dimensional square matrix.
    
    weights : None or n-by-n matrix
        The weight of each distance.  The greater the weight on a distance, the harder SMACOF will try to respect this distance in its solutions.  If None, a matrix of ones is assumed.
    
    init : None or n-by-d matrix
        A starting position for the algorithm.  If None, `n_init` different random positions will be tried, and the best fitting solution will be kept.
    
    anchors : None or m-by-d matrix
        The positions of the m anchors.  If None, it is assumed that there are no anchors.
    
    n_components : int
        The size (i.e. dimensions) of the embedding space.
    
    n_init : int
        The number of initial random positions to try.
    
    maxitr : int
        The maximum number of iterations to run.
    
    eps : float
        The threshold on the stress change between iterations below which convergence is attained.
    
    random_state : None, int or np.RandomState
        The state for the random numbers generator.
    
    Returns
    -------
    n-by-d array :
        The positions of the n samples in the d-dimensional Euclidean space.
    """
    # Default weights are 1's
    if weights is None:
        weights = np.ones(dissimilarities.shape)

    if anchors is None:
        anchors = np.zeros((0, n_components))

    if random_state is None:
        random_state = np.random.RandomState()
    elif type(random_state) == int:
        random_state = np.random.RandomState(random_state)

    # Pre-compute the weights of the Guttman transform
    V = _gt_weights(weights)

    # Only run SMACOF once if an initial position is passed
    if init is not None:
        n_init = 1

    Xbest = None
    Sbest = np.inf
    for itr in range(n_init):
        X, S = _smacof_single(dissimilarities, weights, init, anchors, n_components, maxitr, eps, random_state)

        if S < Sbest:
            Xbest, Sbest = X, S

    return Xbest



