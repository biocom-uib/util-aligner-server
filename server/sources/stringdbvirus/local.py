from contextlib import asynccontextmanager
from os import path

from server.sources.network import read_tsv_edgelist
from server.sources.bitscore import read_tricol_bitscores
from server.sources.source import Source


class StringDBVirusLocalSource(Source):
    def __init__(self, base_path):
        self.base_path = base_path


    # async def get_network_old(self, host_id, virus_id):
    #     network_name = f'{host_id}-{virus_id}'
    #     species_path = path.join(self.base_path, 'networks', f'{host_id}', f'{network_name}.tsv')

    #     if not path.isfile(species_path):
    #         raise LookupError(f'network file for {network_name} was not found')

    #     edgelist = read_tsv_edgelist(path=species_path, header=True)
    #     return StringDBVirusNetwork(host_id, virus_id, edgelist) if edgelist is not None else None


    async def get_network(self, net_desc):
        host_id = net_desc['host_id']
        virus_id = net_desc['virus_id']

        network_name = f'{host_id}-{virus_id}'
        species_path = path.join(self.base_path, 'networks', f'{network_name}.tsv')

        if not path.isfile(species_path):
            raise LookupError(f'network file for {network_name} was not found')

        edgelist = read_tsv_edgelist(path=species_path, header=False)

        if edgelist is None:
            return None
        else:
            return StringDBVirusNetwork(host_id, virus_id, [row[:2] for row in edgelist])


    async def build_custom_network(self, net_desc):
        raise NotImplementedError()


    # async def get_bitscore_matrix_old(self, net1, net2):
    #     # bitscores are supposed to be symmetric
    #     net2 = net2 or net1

    #     matrix_path1 = path.join(self.base_path, 'blast', f'{net1.host_id}-{net2.host_id}', f'{net1.virus_id}-{net2.virus_id}.bitscore.tsv')
    #     matrix_path2 = path.join(self.base_path, 'blast', f'{net1.host_id}-{net2.host_id}', f'{net2.virus_id}-{net1.virus_id}.bitscore.tsv')

    #     try:
    #         return read_tricol_bitscores(matrix_path1, net1=net1, net2=net2, header=True)
    #     except:
    #         return read_tricol_bitscores(matrix_path2, net1=net2, net2=net1, header=True).swapping_net1_net2()


    async def get_bitscore_matrix(self, net1, net2):
        net2 = net2 or net1

        matrix_path = path.join(self.base_path, 'similarity', f'{net1.virus_id}-{net2.virus_id}.tsv')

        return read_tricol_bitscores(matrix_path, net1=net1, net2=net2, header=False)


    async def get_ontology_mapping(self, networks=None):
        return {}



@asynccontextmanager
async def stringdbvirus_local_source(base_path):
    yield StringDBVirusLocalSource(base_path)
