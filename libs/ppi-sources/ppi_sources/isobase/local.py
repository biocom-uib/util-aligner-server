from os import path
import json

from ppi_sources.bitscore import read_tricol_bitscores
from ppi_sources.network import read_net_edgelist_tsv
from ppi_sources.source import Source


class IsobaseLocalSource(Source):
    def __init__(self, base_path):
        self.base_path = base_path


    def _check_valid_species(self, species_name):
        if not species_name.isalnum():
            raise ValueError(f'invalid species name: {species_name}')


    async def get_network(self, net_desc):
        species_name = net_desc['species_name']

        self._check_valid_species(species_name)
        species_path = path.join(self.base_path, f'{species_name}.tab')

        if not path.isfile(species_path):
            raise LookupError(f'network file for {species_name} was not found')

        return read_net_edgelist_tsv(species_name, net_path=species_path)


    async def build_custom_network(self, net_desc):
        raise NotImplementedError()


    async def get_bitscore_matrix(self, net1, net2):
        species1_name = net1.name
        species2_name = net2.name

        self._check_valid_species(species1_name)

        if species2_name is None:
            matrix_path = path.join(self.base_path, f'{species1_name}-blast.tab')

            if not path.isfile(matrix_path):
                raise LookupError(f'score matrix file for {net1.name} was not found')
        else:
            matrix_path = path.join(self.base_path, f'{net1.name}-{net2.name}-blast.tab')

            if not path.isfile(matrix_path):
                raise LookupError(f'score matrix file for {net1.name}-{net2.name} was not found')

        return read_tricol_bitscores(matrix_path, net1=net1, net2=net2)


    async def get_ontology_mapping(self, networks=None):
        with open(path.join(self.base_path, 'go.json'), 'r') as go_f:
            return json.load(go_f)


try:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def isobase_local_source(base_path):
        yield IsobaseLocalSource(base_path)

except ImportError:
    pass
