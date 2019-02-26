from asyncio import coroutine
import numpy as np
from os import path
import pandas as pd
import json

from server.sources.network import read_tsv_edgelist, EdgeListNetwork, VirusHostNetwork
from server.sources.bitscore import read_tricol_bitscores, TricolBitscoreMatrix


class StringDBVirusNetwork(EdgeListNetwork, VirusHostNetwork):
    def __init__(self, host_id, virus_id, edgelist):
        EdgeListNetwork.__init__(self, f'{host_id}-{virus_id}', edgelist)
        VirusHostNetwork.__init__(self, host_name=str(host_id), virus_name=str(virus_id))

        self.host_id = host_id
        self.virus_id = virus_id

    def is_host_vertex(self, v):
        return v['name'].startswith(f'{self.host_id}.')

    def is_virus_vertex(self, v):
        return v['name'].startswith(f'{self.virus_id}.')

    def get_details(self):
        details = {}
        details.update(super().get_details())

        details.update({
            'n_vert_host': self.host_net.igraph.vcount(),
            'n_edges_host': self.host_net.igraph.ecount(),
            'n_vert_virus': self.virus_net.igraph.vcount(),
            'n_edges_virus': self.virus_net.igraph.ecount(),

            'n_interactions': self.igraph.ecount()
                - self.host_net.igraph.ecount()
                - self.virus_net.igraph.ecount(),
        })

        return details


class StringDBVirusLocal(object):
    def __init__(self, base_path):
        self.base_path = base_path

    @coroutine
    def __aenter__(self):
        return self

    @coroutine
    def __aexit__(self, exc_type, exc, tb):
        pass

    @coroutine
    def get_network(self, host_id, virus_id):
        network_name = f'{host_id}-{virus_id}'
        species_path = path.join(self.base_path, 'networks', f'{host_id}', f'{network_name}.tsv')

        if not path.isfile(species_path):
            raise LookupError(f'network file for {network_name} was not found')

        edgelist = read_tsv_edgelist(path=species_path, header=True)
        return StringDBVirusNetwork(host_id, virus_id, edgelist) if edgelist is not None else None

    @coroutine
    def get_bitscore_matrix(self, net1, net2):
        # bitscores are supposed to be symmetric
        net2 = net2 or net1

        matrix_path1 = path.join(self.base_path, 'blast', f'{net1.host_id}-{net2.host_id}', f'{net1.virus_id}-{net2.virus_id}.bitscore.tsv')
        matrix_path2 = path.join(self.base_path, 'blast', f'{net1.host_id}-{net2.host_id}', f'{net2.virus_id}-{net1.virus_id}.bitscore.tsv')

        try:
            return read_tricol_bitscores(matrix_path1, net1=net1, net2=net2, header=True)
        except:
            return read_tricol_bitscores(matrix_path2, net1=net2, net2=net1, header=True).swapping_net1_net2()

    @coroutine
    def get_ontology_mapping(self, networks=None):
        # TODO: Still unclear which annotations should we use for viruses
        return {}
