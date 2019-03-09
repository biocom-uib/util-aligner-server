from networkx.algorithms.distance_measures import eccentricity
from networkx.algorithms.centrality import (
    betweenness_centrality, closeness_centrality, degree_centrality,
    eigenvector_centrality, subgraph_centrality)
from networkx.algorithms.cluster import clustering
SCORES = {}


def score(name):
    def decorator(fnx):
        SCORES[name] = fnx
        return fnx
    return decorator


def compute_score(score, net):
    return score_factory(score)(net)


def score_factory(score):
    if score in SCORES:
        return SCORES[score]
    raise NotImplementedError


@score('DC')
def degree_centrality_score(net):
    return degree_centrality(net)


@score('EC')
def eccentricity_centrality_score(net):
    return {vertex: 1 / e for vertex, e in eccentricity(net).items()}


@score('CC')
def closeness_centrality_score(net):
    return closeness_centrality(net)


@score('EigenC')
def eigenvector_centrality_score(net):
    return eigenvector_centrality(net)


@score('BC')
def betweenness_centrality_score(net):
    return betweenness_centrality(net)


@score('SC')
def subgraph_centrality_score(net):
    return subgraph_centrality(net)


# TO-DO
# @score(SoECC)
def soecc_score(net):
    pass


# TO-DO
# @score('NC')
def neighborhood_centrality_score(net):
    pass


@score('LAC')
def local_average_connectivity_centrality_score(net):
    # Extracted from `A New Method for Identifying Essential Proteins Based on
    # Network Topology Properties and Protein Complexes`
    lac = {}
    for vertex in net:
        number_neighbors = net.degree(vertex)
        subgraph = net.subgraph(net.neighbors(vertex))
        lac[vertex] = 2 * subgraph.size() / number_neighbors
    return lac


@score('LCC')
def local_clustering_coefficient_score(net):
    return clustering(net)


# TO-DO
# @score('ME')
def me_score(net):
    pass
