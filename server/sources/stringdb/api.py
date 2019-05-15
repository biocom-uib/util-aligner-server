from aiohttp import ClientSession
from contextlib import asynccontextmanager
import numpy as np
import pandas as pd

from server.sources.api import SourceAPIClient
from server.sources.source import Source
from server.sources.stringdb.types import StringDBNetwork, StringDBBitscoreMatrix



class StringDBAPISource(Source):
    def __init__(self, http_client):
        super().__init__()

        self.client = SourceAPIClient(http_client, 'stringdb')


    async def get_network(self, net_desc):
        name = await self.client.get_name(net_desc)

        ext_ids = await self.client.request_dataframe('GET', '/db/stringdb/items/proteins', params={
            'columns': ['string_id', 'external_id'],
            'species_id': net_desc['species_id']
        })

        edges_df = await self.client.request_dataframe('GET', '/db/stringdb/network/edges/select', json=net_desc)

        return StringDBNetwork(name, [net_desc['species_id']], ext_ids.set_index('string_id').external_id, edges_df)


    async def build_custom_network(self, net_desc):
        edges_array = np.array(net_desc['edges'], dtype=str)

        vert_ids = np.unique(edge_array.flatten()).tolist()

        vert_data = await self.client.request_dataframe('GET', '/db/stringdb/items/proteins', params={
            'columns': ['string_id', 'species_id', 'external_id'],
            'external_id': list(vert_ids),
        })

        species_ids = vertices.species_id.unique().tolist()

        edges_df = pd.DataFrame(edges_array, columns=['node_id_a', 'node_id_b'])

        if len(ext_ids) < len(vert_ids):
            # TODO: warn for missing ID's
            vert_ids = set(ext_ids.external_id)
            edges_df = edge_df.loc[edge_df.node_id_a.isin(vert_ids) & edge_df.node_id_b.isin(vert_ids)]

        name = await self.client.get_name(net_desc)

        return StringDBNetwork(name, species_ids, ext_ids.set_index('string_id').external_id, edges_df)


    async def get_bitscore_matrix(self, net1, net2):
        df = await self.client.request_dataframe(client, 'POST', '/db/stringdb/bitscore/select', json={
            'net1_species_ids': list(net1.species_ids) or None,
            'net1_protein_ids': list(net1.string_ids),

            'net2_species_ids': list(net2.species_ids) or None,
            'net2_protein_ids': list(net2.string_ids)
        })

        return StringDBBitscoreMatrix(df, net1, net2, by='string_id')


    async def get_ontology_mapping(self, species_ids):
        return await self.client.request_msgpack(client, 'POST', '/db/stringdb/go/select', json={
            'species_ids': species_ids
        })



@asynccontextmanager
async def stringdb_api_source(http_client=None):
    if http_client is None:
        async with ClientSession() as session:
            yield StringDBAPISource(session)
    else:
        yield StringDBAPISource(session)
