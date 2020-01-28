import itertools
import time

@parsl.python_app
def get_consensus(sets, quorum):
    """
    Given an iterable of sets of items, find the set containing all items
    which appear in at least the quorum number of sets.

    Parameters
    ----------
    sets : iterable
        Iterable of sets of items.

    quorum : integer
        Minimum number of sets an item must appear in.

    Returns
    -------
    set
        Set containing all items which appear at least the quorum number of sets.
    """
    if quorum >= 1:
        import itertools

        intersections = [set.intersection(*items) for items in itertools.combinations(sets, quorum)]

        if len(intersections) > 0:
            return set.union(*intersections)
    return {}

