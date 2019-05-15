import igraph
import pandas as pd

from server.util import open_csv_write, iter_csv_fd, iter_csv


class Network(object):
    def __init__(self, name):
        self.name = name
        self._igraph = None

    def get_details(self):
        return {
            f'n_vert': self.igraph.vcount(),
            f'n_edges': self.igraph.ecount(),
        }

    def to_dataframe(self):
        return pd.DataFrame.from_records(self.iter_edges(), columns=['source', 'target'])

    def already_has_igraph(self):
        return self._igraph is not None

    @property
    def igraph(self):
        if self._igraph is None:
            self._igraph = self.to_igraph()
        return self._igraph

    def to_igraph(self):
        raise NotImplementedError()

    def iter_edges(self):
        vs = self.igraph.vs

        for e in self.igraph.es:
            yield vs[e.source]['name'], vs[e.target]['name']

    def iter_vertices(self, by='name'):
        if by == 'object':
            yield from self.igraph.vs
        elif by == 'index':
            yield from range(len(self.igraph.vs))
        else:
            for v in self.igraph.vs:
                yield v[by]

    def write_tsv_edgelist(self, file_path, edgelist=None, delimiter='\t', header=None):
        if edgelist is None:
            edgelist = self.iter_edges()

        with open_csv_write(file_path, delimiter=delimiter) as writer:
            if header is not None:
                writer.writerow(header)
            for edge in edgelist:
                writer.writerow(edge)

    def write_gml(self, file_path):
        for v in self.igraph.vs:
            v['id'] = v.index

        self.igraph.write_gml(file_path)

    def write_leda(self, file_path, names, weights=None):
        self.igraph.write_leda(file_path, names=names, weights=weights)


class EdgeListNetwork(Network):
    def __init__(self, name, edgelist):
        super().__init__(name)
        self.edgelist = edgelist

    def to_igraph(self):
        g = igraph.Graph.TupleList(self.iter_edges())
        g.simplify()
        return g

    def iter_edges(self):
        return self.edgelist if not self.already_has_igraph() else super().iter_edges()


def read_tsv_edgelist(*, path=None, string=None, header=False):
    if string is not None:
        return list(iter_csv_fd(io.StringIO(string), delimiter='\t', header=header))
    elif path is not None:
        return list(iter_csv(path, delimiter='\t', header=header))
    else:
        return None


def read_net_tsv_edgelist(name, *, net_path=None, string=None, header=False):
    edgelist = read_tsv_edgelist(path=net_path, string=string, header=header)

    return EdgeListNetwork(name, edgelist) if edgelist is not None else None


class IgraphNetwork(Network):
    def __init__(self, name, graph, simplify=True):
        super().__init__(name)
        self.graph = graph
        if simplify:
            self.graph.simplify()

    def to_igraph(self):
        return self.graph


def read_net_gml(name, net_path):
    return IgraphNetwork(name, igraph.Graph.Read_GML(net_path))


class VirusHostNetwork(object):
    def __init__(self, host_name, virus_name):
        self._host_name = host_name
        self._virus_name = virus_name

        self._host_net = None
        self._virus_net = None
        self._vh_bipartite_net = None

    def is_virus_vertex(self, vid):
        raise NotImplementedError()

    def is_host_vertex(self, vid):
        raise NotImplementedError()

    def is_vh_interaction_edge(self, e):
        vs = self.igraph.vs
        src, tgt = e.tuple
        return self.is_virus_vertex(vs[src]) != self.is_virus_vertex(vs[tgt])


    @property
    def host_net(self):
        if self._host_net is None:
            self._host_net = IgraphNetwork(self._host_name, self.igraph.vs.select(self.is_host_vertex).subgraph())
        return self._host_net

    @property
    def virus_net(self):
        if self._virus_net is None:
            self._virus_net = IgraphNetwork(self._virus_name, self.igraph.vs.select(self.is_virus_vertex).subgraph())
        return self._virus_net

    @property
    def vh_bipartite_net(self):
        if self._vh_bipartite_net is None:
            self._vh_bipartite_net = IgraphNetwork(
                    f'{self._host_name}-{self._virus_name}',
                    self.igraph.es.select(self.is_vh_interaction_edge).subgraph())

        return self._vh_bipartite_net
