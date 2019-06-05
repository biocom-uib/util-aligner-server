from hashlib import sha1
import json
import pandas as pd

from ppi_sources.source import Source
from ppi_sources.stringdb.stringdb import StringDB
from ppi_sources.stringdb.types import StringDBNetwork, StringDBBitscoreMatrix


class StringDBPostgresSource(Source):
    def __init__(self, db=None):
        super().__init__()

        self.db = db


    async def get_network(self, net_desc):
        # name = sha1(json.dumps(net_desc, sort_keys=True)).hexdigest()
        name = f"stringdb_{net_desc['species_id']}"

        edges_df = await self.db.get_network(net_desc['species_id'], net_desc['score_thresholds'])

        ext_ids = await self.db.get_protein_external_ids(net_desc['species_id'])
        ext_ids = pd.Series(ext_ids).rename('external_id').rename_axis(index='string_id')

        return StringDBNetwork(name, [net_desc['species_id']], ext_ids, edges_df)


    async def build_custom_network(self, net_desc):
        raise NotImplementedError()


    async def get_bitscore_matrix(self, net1, net2):
        df = await self.db.get_bitscore_matrix(net1.species_ids, net1.string_ids, net2.species_ids, net2.string_ids)

        return StringDBBitscoreMatrix(df, net1, net2, by='string_id')


    async def get_ontology_mapping(self, networks):
        species_ids = {species_id for network in networks for species_id in network.species_ids}

        return await self.db.get_ontology_mapping(tuple(species_ids))


try:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def stringdb_postgres_source(**kwargs):
        async with StringDB(**kwargs) as db:
            yield StringDBPostgresSource(db)

except ImportError:
    pass
