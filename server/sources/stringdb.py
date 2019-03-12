import aiopg
import igraph
import numpy as np

from server.sources.network import Network
from server.sources.bitscore import TricolBitscoreMatrix


class StringDBNetwork(Network):
    def __init__(self, species_id, external_ids, edges):
        super().__init__(f'stringdb_{species_id}')
        self.species_id = species_id
        self.external_ids = external_ids
        self.edges = edges

        if species_id >= 0:
            self._species = [species_id]
        else:
            self._species = []

    def to_igraph(self):
        # NOTE: igraph has some bugs regarding non-str names
        # (https://github.com/igraph/python-igraph/issues/73#issuecomment-203077381)

        ext_ids = self.external_ids

        graph = igraph.Graph.TupleList(self.edges)

        for v in graph.vs:
            string_id = int(v['name']) # better have unique names
            ext_id = ext_ids[string_id]

            v['name'] = ext_id
            v['string_id'] = string_id
            v['external_id'] = ext_id

        if not graph.is_simple():
            graph.simplify()

        return graph

    async def get_species(self, db):
        if not self._species:
            self._species = await db.get_species(self.string_ids)

        return self._species

    @property
    def string_ids(self):
        return {int(v) for v in self.iter_vertices(by='string_id')}


class StringDBBitscoreMatrix(TricolBitscoreMatrix):
    def __init__(self, tricol, net1, net2, by='string_id'):
        super().__init__(tricol, net1, net2, by)

    def iter_tricol(self, by='name'):
        # just an optimization
        if by == 'name':
            ext_ids1 = net1.external_ids
            ext_ids2 = net2.external_ids

            for p1, p2, score in super().iter_tricol(by='string_id'):
                yield ext_ids1[p1], ext_ids2[p2], score
        else:
            yield from super().iter_tricol(by=by)


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

    async def get_protein_external_ids(self, species_id):
        async with self._get_cursor() as cursor:
            await cursor.execute("""
                select protein_id, protein_external_id
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

    async def get_network(self, species_id, score_thresholds={}, external_ids=None):
        if external_ids is None:
            external_ids = await self.get_protein_external_ids(species_id)

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

        return StringDBNetwork(species_id, external_ids, np.array(edges))

    async def build_custom_network(self, edges):
        string_ids = {p for e in edges for p in e}
        missing = await self._check_string_ids(string_ids)

        if not missing:
            return StringDBNetwork(-1, await self.get_protein_external_ids(string_ids=string_ids), np.array(edges))
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
