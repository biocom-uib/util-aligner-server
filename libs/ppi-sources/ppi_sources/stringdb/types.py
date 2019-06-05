import igraph

from ppi_sources.network import Network
from ppi_sources.bitscore import TricolBitscoreMatrix


class StringDBNetwork(Network):
    def __init__(self, name, species_ids, external_ids, string_id_edges_df):
        super().__init__(name)

        self._string_id_edges = string_id_edges_df.astype(str)

        self.species_ids = species_ids
        self.external_ids = external_ids.rename('external_id').rename_axis(index='string_id')


    def to_igraph(self):
        # NOTE: igraph has some bugs regarding non-str names
        # (https://github.com/igraph/python-igraph/issues/73#issuecomment-203077381)

        ext_ids = self.external_ids

        graph = igraph.Graph.TupleList(self._string_id_edges.to_numpy(copy=False))

        for v in graph.vs:
            string_id = int(v['name']) # better have unique names
            ext_id = ext_ids[string_id]

            v['name'] = ext_id
            v['string_id'] = string_id
            v['external_id'] = ext_id

        if not graph.is_simple():
            graph.simplify()

        return graph


    @property
    def string_ids(self):
        return {int(v) for v in self.iter_vertices(by='string_id')}


class StringDBBitscoreMatrix(TricolBitscoreMatrix):
    def __init__(self, tricol, net1, net2, by='string_id'):
        super().__init__(tricol, net1, net2, by)


    def iter_tricol(self, by='name'):
        # just an optimization
        if by == 'name':
            ext_ids1 = self.net1.external_ids
            ext_ids2 = self.net2.external_ids

            for p1, p2, score in super().iter_tricol(by='string_id'):
                yield ext_ids1[p1], ext_ids2[p2], score
        else:
            yield from super().iter_tricol(by=by)

