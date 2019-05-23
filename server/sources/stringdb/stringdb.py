import aiopg
import numpy as np
import pandas as pd


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


    async def get_network(self, species_id, score_thresholds={}):
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

        if edges:
            return pd.DataFrame(np.array(edges, dtype='i4'), columns=['node_id_a', 'node_id_b'])
        else:
            raise LookupError('retrieved empty network')


    async def get_bitscore_matrix(self, net1_species_ids, net1_string_ids, net2_species_ids, net2_string_ids):
        async with self._get_cursor() as cursor:
            await cursor.execute("""
                with
                    net1_prot_ids as (select unnest(%(net1_protein_ids)s :: integer[]) net1_prot_id),
                    net2_prot_ids as (select unnest(%(net2_protein_ids)s :: integer[]) net2_prot_id)
                select
                  protein_id_a, protein_id_b, bitscore
                from
                  homology.blast_data blast
                where
                  species_id_a in %(net1_species_ids)s
                  and
                  species_id_b in %(net2_species_ids)s
                  and
                  protein_id_a in (select net1_prot_id from net1_prot_ids)
                  and
                  protein_id_b in (select net2_prot_id from net2_prot_ids);
                """,
                {'net1_species_ids': tuple(net1_species_ids),
                 'net2_species_ids': tuple(net2_species_ids),
                 'net1_protein_ids': list(net1_string_ids),
                 'net2_protein_ids': list(net2_string_ids)})

            values = await cursor.fetchall()

        if values:
            return pd.DataFrame(values, columns=['protein_id_a', 'protein_id_b', 'bitscore'])
        else:
            raise LookupError('bitscore matrix not available for the selected network pair')


    async def get_ontology_mapping(self, species_ids):
        async with self._get_cursor() as cursor:
            await cursor.execute("""
                select
                  p.protein_external_id,
                  array_agg(distinct g.go_id)
                from
                  mapping.gene_ontology g
                inner join
                  items.proteins p on p.protein_id = g.string_id
                where
                  g.species_id in %(species_ids)s
                  and
                  g.evidence_code in ('EXP', 'IDA', 'IPI', 'IMP', 'IGI', 'IEP', 'IC')
                group by
                  p.protein_external_id;
                """,
                {'species_ids': tuple(species_ids)})

            rows = await cursor.fetchall()

        return dict(rows)


    async def get_string_go_annotations(self, protein_ids=None, taxid=None):
        if protein_ids is not None:
            async with self._get_cursor() as cursor:
                await cursor.execute("""
                    select protein_id, go_id
                    from go.explicit_by_id
                    where protein_id = ANY(%(protein_ids)s);
                    """,
                    {'protein_ids': protein_ids})

                rows = await cursor.fetchall()

            return rows if rows else None

        if taxid is not None:
            async with self._get_cursor() as cursor:
                await cursor.execute("""
                    select gos.protein_id, gos.go_id
                    from (select *
                          from items.proteins
                          where species_id = %(species_id)s) as proteins
                    left join go.explicit_by_id as gos
                        on proteins.protein_id = gos.protein_id;
                    """,
                    {'species_id': taxid})

                rows = await cursor.fetchall()

            return rows if rows else None
