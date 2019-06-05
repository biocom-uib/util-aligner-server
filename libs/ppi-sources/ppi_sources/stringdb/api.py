from aiohttp import ClientSession, TCPConnector
import numpy as np
import pandas as pd

from ppi_sources.api import SourceAPIClient
from ppi_sources.source import Source
from ppi_sources.stringdb.types import StringDBNetwork, StringDBBitscoreMatrix



class StringDBAPISource(Source):
    def __init__(self, http_client, host):
        super().__init__()

        self.client = SourceAPIClient(http_client, host, 'stringdb')


    async def get_network(self, net_desc):
        name = f"stringdb_{net_desc['species_id']}"
        #name = await self.client.get_name(net_desc)

        ext_ids = await self.client.request_dataframe('POST', '/db/stringdb/items/proteins/select', json={
            'columns': ['protein_id', 'protein_external_id'],
            'filter': {
                'species_id': [net_desc['species_id']]
            }
        })
        ext_ids = ext_ids.rename(columns={
            'protein_id': 'string_id',
            'protein_external_id': 'external_id'
        })

        edges_df = await self.client.request_dataframe('POST', '/db/stringdb/network/edges/select', json=net_desc)

        return StringDBNetwork(name,
                [net_desc['species_id']],
                ext_ids.set_index('string_id').external_id,
                edges_df)


    async def build_custom_network(self, net_desc):
        edges_array = np.array(net_desc['edges'], dtype=str)

        vert_ids = np.unique(edge_array.flatten()).tolist()

        vert_data = await self.client.request_dataframe('POST', '/db/stringdb/items/proteins/select', json={
            'columns': ['protein_id', 'species_id', 'protein_external_id'],
            'filter': {
                'protein_external_id': vert_ids
            }
        })
        vert_data = vert_data.rename(columns={
            'protein_id': 'string_id',
            'protein_external_id': 'external_id'
        })

        species_ids = vertices.species_id.unique().tolist()

        edges_df = pd.DataFrame(edges_array, columns=['node_id_a', 'node_id_b'])

        if len(ext_ids) < len(vert_ids):
            # TODO: warn for missing ID's
            vert_ids = set(ext_ids.external_id)
            edges_df = edge_df.loc[edge_df.node_id_a.isin(vert_ids) & edge_df.node_id_b.isin(vert_ids)]

        name = f"stringdb_{species_id[0]}"
        #name = await self.client.get_name(net_desc)

        return StringDBNetwork(name, species_ids, ext_ids.set_index('string_id').external_id, edges_df)


    async def get_bitscore_matrix(self, net1, net2):
        df = await self.client.request_dataframe('POST', '/db/stringdb/bitscore/select', json={
            'net1_species_ids': list(net1.species_ids),
            'net1_protein_ids': list(net1.string_ids),

            'net2_species_ids': list(net2.species_ids),
            'net2_protein_ids': list(net2.string_ids)
        })
        df = df.rename(columns={
            'protein_id_a': 'protein_id_a',
            'protein_id_b': 'protein_id_b',
            'bitscore': 'bitscore'
        })

        return StringDBBitscoreMatrix(df, net1, net2, by='string_id')


    async def get_ontology_mapping(self, networks):
        species_ids = {species_id for network in networks for species_id in network.species_ids}

        return await self.client.request_msgpack('POST', '/db/stringdb/go/annotations/select', json={
            'species_ids': list(species_ids)
        })


try:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def stringdb_api_source(host=None, *, timeout=SourceAPIClient.DEFAULT_TIMEOUT, http_client=None):
        if http_client is None:
            async with ClientSession(timeout=timeout) as session:
                yield StringDBAPISource(session, host=host)
        else:
            yield StringDBAPISource(session, host=host)

except ImportError:
    pass
