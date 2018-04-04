#!/usr/bin/python3
import igraph
import io
import json
from lazy_property import LazyProperty
import numpy as np
# import psycopg2
import os
from os import path

import util



class Network(object):
    def __init__(self):
        pass

    @LazyProperty
    def igraph(self):
        return self.to_igraph()

    def iter_edges(self):
        vs = self.igraph.vs

        for e in self.graph.es:
            yield vs[e.source]['name'], vs[e.target]['name']

    def iter_vertices(self, by='name'):
        if by == 'object':
            yield from self.igraph.vs
        elif by == 'index':
            yield from range(len(self.igraph.vs))
        elif by == 'name':
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
    def __init__(self, edgelist):
        super().__init__()
        self.edgelist = edgelist

    def to_igraph(self):
        return igraph.Graph.TupleList(self.iter_edges())

    def iter_edges(self):
        return self.edgelist

class IgraphNetwork(Network):
    def __init__(self, graph, simplify=True):
        super().__init__()
        self.graph = graph
        if simplify:
            self.graph.simplify()

    def to_igraph(self):
        return self.graph

class StringDBNetwork(Network):
    def __init__(self, proteins, edges):
        self.proteins = proteins
        self.edges = edges

    def to_igraph(self):
        graph = igraph.Graph.TupleList(self.iter_edges())
        if not graph.is_simple():
            graph.simplify()

        prots = self.proteins
        for v in graph.vs:
            string_id = v['name'] # better have unique names
            v['name'] = str(string_id) # igraph has some bugs regarding non-str names (https://github.com/igraph/python-igraph/issues/73#issuecomment-203077381)
            v['prot_name'] = prots[string_id] # not necessarily unique, e.g. NGR_c13120

        return graph

    def iter_edges(self):
        return self.edges[:,:2]

def read_net_gml(net_path):
    return IgraphNetwork(igraph.Graph.Read_GML(net_path))

def read_net_tsv_edgelist(net_path=None, string=None, simplify=True):
    if string is not None:
        return EdgeListNetwork(list(util.iter_csv_fd(io.StringIO(string), delimiter='\t')))
    elif net_path is not None:
        return EdgeListNetwork(list(util.iter_csv(net_path, delimiter='\t')))
    else:
        return None


class ScoreMatrix(object):
    def __init__(self):
        pass

    def write_tricol(self, file_path, by='name', **kwargs):
        if 'delimiter' not in kwargs:
            kwargs['delimiter'] = '\t'
        util.write_csv(file_path, self.iter_tricol(by=by), **kwargs)

class TricolScoreMatrix(ScoreMatrix):
    def __init__(self, tricol, net1=None, net2=None, by='name'):
        self.tricol = np.array(list(tricol))
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


def read_tricol_scores(file_path, net1=None, net2=None, by='name', **kwargs):
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = '\t'
    return TricolScoreMatrix(util.iter_csv(file_path, **kwargs), net1=net1, net2=net2, by=by)

def identity_scores(net, by='name'):
    return TricolScoreMatrix([(v, v, 1) for v in net.iter_vertices(by=by)], by=by)


class IsobaseLocal(object):
    def __init__(self, base_path):
        self.base_path = base_path

    def _check_valid_species(self, species_name):
        if not species_name.isalnum():
            raise ValueError(f'invalid species name: {species_name}')

    def get_network(self, species_name):
        self._check_valid_species(species_name)
        species_path = path.join(self.base_path, f'{species_name}.tab')

        if not path.isfile(species_path):
            raise IOError(f'network file for {species_name} was not found')

        return read_net_tsv_edgelist(species_path)

    def get_score_matrix(self, species1_name, species2_name=None, net1=None, net2=None):
        self._check_valid_species(species1_name)

        if species2_name is None:
            matrix_path = path.join(self.base_path, f'{species1_name}-blast.tab')

            if not path.isfile(matrix_path):
                raise IOError(f'score matrix file for {species1_name} was not found')
        else:
            self._check_valid_species(species2_name)
            matrix_path = path.join(self.base_path, f'{species1_name}-{species2_name}-blast.tab')

            if not path.isfile(matrix_path):
                raise IOError(f'score matrix file for {species1_name}-{species2_name} was not found')

        return read_tricol_scores(matrix_path, net1=net1, net2=net2)

    def get_ontology_mapping(self, networks=None):
        with open(path.join(self.base_path, 'go.json'), 'r') as go_f:
            return json.load(go_f)

class StringDB(object):
    def __init__(self, host='localhost', port=5432, user='ppin', dbname='stringdb_raw'):
        self.host = host
        self.port = port
        self.user = user
        self.dbname = dbname

    def connect(self):
        self.conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, dbname=self.dbname)

    def disconnect(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self):
        self.disconnect()

    def get_proteins(self, species_id):
        with self.conn.cursor() as cursor:
            cursor.execute("""
                select protein_id, preferred_name
                from items.proteins
                where species_id = %s;
                """,
                (species_id,))
            return dict(cursor)

    def get_protein_sequences(self, species_id):
        with self.conn.cursor() as cursor:
            cursor.execute("""
                select proteins.protein_id, sequences.sequence
                from items.proteins as proteins
                inner join items.proteins_sequences as sequences
                    on proteins.protein_id = sequences.protein_id
                where proteins.species_id = %s;
                """,
                (species_id,))
            return dict(cursor)

    def get_network(self, species_id, min_combined_score=0, proteins=None):
        if proteins is None:
            proteins = self.get_proteins(species_id)

        with self.conn.cursor() as cursor:
            cursor.execute("""
                select node_id_a, node_id_b, combined_score
                from network.node_node_links
                where node_type_b = %s and combined_score >= %s;
                """,
                (species_id, min_combined_score))
            return StringDBNetwork(proteins, np.array(cursor.fetchall()))

    def get_score_matrix(self, net1_taxid, net2_taxid):
        net1 = self.get_network(net1_taxid)
        net2 = self.get_network(net2_taxid) if net1_taxid != net2_taxid else net1

        def fetch_all_homology(cursor):
            cursor.execute("""
                select blast.protein_id_a, blast.protein_id_b, blast.bitscore
                from (select protein_id
                      from items.proteins
                      where species_id = %s) as proteins_a
                left join homology.similarity_data as blast
                    on proteins_a.protein_id = blast.protein_id_a
                where species_id_b = %s;
                """, (net1_taxid, net2_taxid))

            return cursor.fetchall()

        with self.conn.cursor() as cursor:
            values = np.array(list(fetch_all_homology(cursor)))
            return TricolScoreMatrix(values, net1=net1, net2=net2, by='name')

    def get_string_go_annotations(self, protein_ids=None, taxid=None):
        if protein_ids is not None:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    select protein_id, go_id
                    from go.explicit_by_id
                    where protein_id = ANY(%s);
                    """, (protein_ids,))
                return cursor.fetchall()

        if taxid is not None:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    select gos.protein_id, gos.go_id
                    from (select *
                          from items.proteins
                          where species_id = %s) as proteins
                    left join go.explicit_by_id as gos
                        on proteins.protein_id = gos.protein_id;
                    """, (taxid,))
                return cursor.fetchall()
