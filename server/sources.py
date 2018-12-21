#!/usr/bin/python3
from asyncio import coroutine
import igraph
import io
import json
import numpy as np
import aiopg
from os import path

import util


class Network(object):
    def __init__(self, name):
        self.name = name
        self._igraph = None

    def already_has_igraph(self):
        return self._igraph is not None

    @property
    def igraph(self):
        if self._igraph is None:
            self._igraph = self.to_igraph()
        return self._igraph

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

        with util.open_csv_write(file_path, delimiter=delimiter) as writer:
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
        return igraph.Graph.TupleList(self.iter_edges())

    def iter_edges(self):
        return self.edgelist if not self.already_has_igraph() else super().iter_edges()

class IgraphNetwork(Network):
    def __init__(self, name, graph, simplify=True):
        super().__init__(name)
        self.graph = graph
        if simplify:
            self.graph.simplify()

    def to_igraph(self):
        return self.graph

class StringDBNetwork(Network):
    def __init__(self, species_id, protein_names, edges):
        super().__init__(f'stringdb_{species_id}')
        self.species_id = species_id
        self.protein_names = protein_names
        self._protein_set = None
        self.edges = edges

        if species_id >= 0:
            self._species = [species_id]
        else:
            self._species = []
        self._protein_set

    def to_igraph(self):
        graph = igraph.Graph.TupleList(self.iter_edges())
        if not graph.is_simple():
            graph.simplify()

        prots = self.protein_names
        for v in graph.vs:
            string_id = v['name'] # better have unique names
            # igraph has some bugs regarding non-str names (https://github.com/igraph/python-igraph/issues/73#issuecomment-203077381)
            v['name'] = f'v{string_id}'
            v['string_id'] = string_id
            v['prot_name'] = prots[string_id] # not necessarily unique, e.g. NGR_c13120

        return graph

    async def get_species(self, db):
        if not self._species:
            self._species = await db.get_species(self.string_ids)

        return self._species

    @property
    def string_ids(self):
        return {int(v) for v in self.iter_vertices(by='string_id')}

    def iter_edges(self):
        return self.edges if not self.already_has_igraph() else super().iter_edges()

def read_net_gml(name, net_path):
    return IgraphNetwork(name, igraph.Graph.Read_GML(net_path))

def read_net_tsv_edgelist(name, net_path=None, string=None, simplify=True):
    if string is not None:
        return EdgeListNetwork(name, list(util.iter_csv_fd(io.StringIO(string), delimiter='\t')))
    elif net_path is not None:
        return EdgeListNetwork(name, list(util.iter_csv(net_path, delimiter='\t')))
    else:
        return None


class BitscoreMatrix(object):
    def __init__(self):
        pass

    def write_tricol(self, file_path, by='name', **kwargs):
        if 'delimiter' not in kwargs:
            kwargs['delimiter'] = '\t'
        util.write_csv(file_path, self.iter_tricol(by=by), **kwargs)

class TricolBitscoreMatrix(BitscoreMatrix):
    def __init__(self, tricol, net1=None, net2=None, by='name'):
        self.tricol = np.array(tricol)
        self.net1 = net1
        self.net2 = net2
        self.by = by

    def iter_tricol(self, by='name'):
        self_by = self.by

        if by == self_by:
            yield from self.tricol

        elif by == 'object':
            if self.net1 is None or self.net2 is None:
                raise ValueError(f"must specify net1 and net2 in order to use different 'by' values (current: {self.by}, given: {by})")

            net1_vs = self.net1.igraph.vs
            net2_vs = self.net2.igraph.vs

            if self_by == 'index':
                for p1_id, p2_id, score in self.tricol:
                    yield net1_vs[p1_id], net2_vs[p2_id], score
            else:
                for p1_by, p2_by, score in self.tricol:
                    p1s = net1_vs.select(**{self_by:p1_by})
                    p2s = net2_vs.select(**{self_by:p2_by})
                    if len(p1s) > 0 and len(p2s) > 0:
                        yield p1s[0], p2s[0], score
        elif by == 'index':
            for p1, p2, score in self.iter_tricol(by='object'):
                yield p1.index, p2.index, score
        else:
            for p1, p2, score in self.iter_tricol(by='object'):
                yield p1[by], p2[by], score

class StringDBBitscoreMatrix(TricolBitscoreMatrix):
    def __init__(self, tricol, net1=None, net2=None, by='string_id'):
        super().__init__(tricol, net1, net2, by)

    def iter_tricol(self, by='name'):
        # just an optimization
        if by == 'name':
            for p1, p2, score in super().iter_tricol(by='string_id'):
                yield f'v{p1}', f'v{p2}', score
        else:
            yield from super().iter_tricol(by=by)


def read_tricol_bitscores(file_path, net1=None, net2=None, by='name', **kwargs):
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = '\t'
    return TricolBitscoreMatrix(list(util.iter_csv(file_path, **kwargs)), net1=net1, net2=net2, by=by)

def identity_bitscores(net, by='name'):
    return TricolBitscoreMatrix([(v, v, 1) for v in net.iter_vertices(by=by)], by=by)


class IsobaseLocal(object):
    def __init__(self, base_path):
        self.base_path = base_path

    def _check_valid_species(self, species_name):
        if not species_name.isalnum():
            raise ValueError(f'invalid species name: {species_name}')

    @coroutine
    def __aenter__(self):
        return self

    @coroutine
    def __aexit__(self, exc_type, exc, tb):
        pass

    @coroutine
    def get_network(self, species_name):
        self._check_valid_species(species_name)
        species_path = path.join(self.base_path, f'{species_name}.tab')

        if not path.isfile(species_path):
            raise LookupError(f'network file for {species_name} was not found')

        return read_net_tsv_edgelist(species_name, species_path)

    @coroutine
    def get_bitscore_matrix(self, species1_name, species2_name=None, net1=None, net2=None):
        self._check_valid_species(species1_name)

        if species2_name is None:
            matrix_path = path.join(self.base_path, f'{species1_name}-blast.tab')

            if not path.isfile(matrix_path):
                raise LookupError(f'score matrix file for {species1_name} was not found')
        else:
            self._check_valid_species(species2_name)
            matrix_path = path.join(self.base_path, f'{species1_name}-{species2_name}-blast.tab')

            if not path.isfile(matrix_path):
                raise LookupError(f'score matrix file for {species1_name}-{species2_name} was not found')

        return read_tricol_bitscores(matrix_path, net1=net1, net2=net2)

    @coroutine
    def get_ontology_mapping(self, networks=None):
        with open(path.join(self.base_path, 'go.json'), 'r') as go_f:
            return json.load(go_f)

class StringDB(object):
    EVIDENCE_SCORE_TYPES = {
        'equiv_nscore':                    1,
        'equiv_nscore_transferred':        2,
        'equiv_fscore':                    3,
        'equiv_pscore':                    4,
        'equiv_hscore':                    5,
        'array_score':                     6,
        'array_score_transferred':         7,
        'experimental_score':              8,
        'experimental_score_transferred':  9,
        'database_score':                  10,
        'database_score_transferred':      11,
        'textmining_score':                12,
        'textmining_score_transferred':    13,
        'neighborhood_score':              14,
        'fusion_score':                    15,
        'cooccurence_score':               16
    }

    @staticmethod
    async def init_pool(host='stringdb', port=5432, user='stringdb', password='stringdb', dbname='stringdb'):
        return await aiopg.create_pool(host=host, port=port, user=user, password=password, dbname=dbname, timeout=None)

    def __init__(self, host='stringdb', port=5432, user='stringdb', password='stringdb', dbname='stringdb', pool=None):
        self.pool = pool
        self.conn = None

        if pool is None:
            self.host = host
            self.port = port
            self.user = user
            self.password = password
            self.dbname = dbname

    async def connect(self):
        if self.pool is None:
            self.conn = await aiopg.connect(host=self.host, port=self.port, user=self.user, password=self.password, dbname=self.dbname, timeout=None)

    async def disconnect(self):
        if self.conn is not None and not self.conn.closed:
            await self.conn.close()
            self.conn = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.disconnect()

    def _get_cursor(self):
        if self.pool is not None:
            return self.pool.cursor()
        else:
            return self.conn.cursor()

    async def get_species(self, string_ids):
        async with self._get_cursor() as cursor:
            await cursor.execute("""
                select distinct species_id
                from items.proteins
                where protein_id in %(string_ids)s;
                """,
                {'string_ids': tuple(string_ids)})

            rows = await cursor.fetchall()

        return [s for s, in rows]

    async def _check_string_ids(self, string_ids):
        async with self._get_cursor() as cursor:
            await cursor.execute("""
                select
                  string_id
                from
                  (select unnest(%(string_ids)s) as string_id) string_ids
                where
                  not exists (
                    select protein_id
                    from items.proteins
                    where protein_id = string_id
                  );""",
                  {'string_ids': list(string_ids)})

            rows = await cursor.fetchall()

        # return invalid string_id's
        return [string_id for string_id, in rows]

    async def get_protein_names(self, species_id=None, string_ids=None):
        async with self._get_cursor() as cursor:
            if string_ids is not None:
                await cursor.execute("""
                    select protein_id, preferred_name
                    from items.proteins
                    where protein_id in %(string_ids)s;
                    """,
                    {'string_ids': tuple(string_ids)})
            else:
                await cursor.execute("""
                    select protein_id, preferred_name
                    from items.proteins
                    where species_id = %(species_id)s;
                    """,
                    {'species_id': species_id})

            rows = await cursor.fetchall()

        return dict(rows)

    async def get_protein_sequences(self, species_id):
        async with self._get_cursor() as cursor:
            await cursor.execute("""
                select proteins.protein_id, sequences.sequence
                from items.proteins as proteins
                inner join items.proteins_sequences as sequences
                    on proteins.protein_id = sequences.protein_id
                where proteins.species_id = %(species_id)s;
                """,
                {'species_id': species_id})

            rows = await cursor.fetchall()

        return dict(rows)

    async def get_network(self, species_id, score_thresholds={}, protein_names=None):
        if protein_names is None:
            protein_names = await self.get_protein_names(species_id)

        async with self._get_cursor() as cursor:
            if score_thresholds:
                sql = """
                    with indexed as
                      (select
                         node_id_a,
                         node_id_b,
                         evidence_scores,
                         generate_subscripts(evidence_scores, 1) i
                       from
                         network.node_node_links
                       where
                         node_type_b = %(species_id)s)
                    select distinct
                      node_id_a,
                      node_id_b
                    from
                      indexed
                    where
                      false
                    """

                for score_type, threshold in score_thresholds.items():
                    if score_type in StringDB.EVIDENCE_SCORE_TYPES and isinstance(threshold, int):
                        score_id = StringDB.EVIDENCE_SCORE_TYPES[score_type]
                        sql += f'\nor (evidence_scores[i][1] = {score_id} and evidence_scores[i][2] >= {threshold})'
                    else:
                        print(f'StringDB.get_network: invalid score_type/threshold pair: {score_type} >= {threshold}')

                sql += ';'

            else:
                sql = """
                    select
                      node_id_a, node_id_b
                    from
                      network.node_node_links
                    where
                      node_type_b = %(species_id)s;
                    """;


            await cursor.execute(sql, {'species_id': species_id})

            edges = await cursor.fetchall()

        return StringDBNetwork(species_id, protein_names, np.array(edges))

    async def build_custom_network(self, edges):
        string_ids = {p for e in edges for p in e}
        missing = await self._check_string_ids(string_ids)

        if not missing:
            return StringDBNetwork(-1, await self.get_protein_names(string_ids=string_ids), np.array(edges))
        else:
            raise ValueError(f"Invalid string_id's: {set(missing)}")

    async def get_bitscore_matrix(self, net1, net2):
        async with self._get_cursor() as cursor:
            await cursor.execute("""
                with
                    net1_protein_ids as (select unnest(%(net1_protein_ids)s :: integer[]) net1_prot_ids),
                    net2_protein_ids as (select unnest(%(net2_protein_ids)s :: integer[]) net2_prot_ids)
                select
                  protein_id_a, protein_id_b, bitscore
                from
                  homology.blast_data blast
                  inner join
                    net1_protein_ids on (blast.protein_id_a = net1_protein_ids.net1_prot_ids)
                  inner join
                    net2_protein_ids on (blast.protein_id_b = net2_protein_ids.net2_prot_ids)
                where
                  species_id_a in %(net1_species_ids)s
                  and
                  species_id_b in %(net2_species_ids)s
                """,
                {'net1_species_ids': tuple(await net1.get_species(self)),
                 'net2_species_ids': tuple(await net2.get_species(self)),
                 'net1_protein_ids': list(net1.string_ids),
                 'net2_protein_ids': list(net2.string_ids)})

            values = await cursor.fetchall()

        array_dtype = [('protein_id_a', 'i4'), ('protein_id_b', 'i4'), ('bitscore', 'f4')]

        if values:
            return StringDBBitscoreMatrix(np.array(values, dtype=array_dtype), net1=net1, net2=net2, by='string_id')
        else:
            raise LookupError('bitscore matrix not available for the selected network pair')

    async def get_ontology_mapping(self, networks):
        species_ids = [species for net in networks
                               for species in await net.get_species(self)]

        async with self._get_cursor() as cursor:
            await cursor.execute("""
                select
                  'v' || string_id,
                  array_agg(distinct go_id)
                from
                  mapping.gene_ontology
                where
                  species_id in %(species_ids)s
                  and
                  evidence_code in ('EXP', 'IDA', 'IPI', 'IMP', 'IGI', 'IEP', 'IC')
                group by
                  string_id;
                """,
                {'species_ids': tuple(species_ids)})

            rows = await cursor.fetchall()

        return dict(rows)

    async def get_string_go_annotations(self, protein_ids=None, taxid=None):
        if protein_ids is not None:
            async with self._get_cursor() as cursor:
                await cursor.execute("""
                    select 'v' || protein_id, go_id
                    from go.explicit_by_id
                    where protein_id = ANY(%(protein_ids)s);
                    """,
                    {'protein_ids': protein_ids})

                rows = await cursor.fetchall()

            return rows if rows else None

        if taxid is not None:
            async with self._get_cursor() as cursor:
                await cursor.execute("""
                    select 'v' || gos.protein_id, gos.go_id
                    from (select *
                          from items.proteins
                          where species_id = %(species_id)s) as proteins
                    left join go.explicit_by_id as gos
                        on proteins.protein_id = gos.protein_id;
                    """,
                    {'species_id': taxid})

                rows = await cursor.fetchall()

            return rows if rows else None
