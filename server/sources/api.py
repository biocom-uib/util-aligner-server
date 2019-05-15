from aiohttp import ClientTimeout
from msgpack import unpackb
import pandas as pd
from yarl import URL


NO_TIMEOUT = ClientTimeout()


class SourceAPIClient(object):
    def __init__(self, http_client, source_name):
        self.client = http_client
        self.source_name = source_name


    async def request_msgpack(self, method, url, *, params={}, **kwargs):
        headers = {'Accept': 'application/x-msgpack'}

        async with self.client.request(method, url, params, headers=headers, raise_for_status=True, **kwargs) as req:
            return unpackb(await req.read())


    async def request_dataframe(self, method, url, *, params={}, **kwargs):
        return pd.DataFrame(await self.request_msgpack(method, url, *, params={}, **kwargs))


    async def request_tsv(self, method, url, *, params={}, **kwargs):
        headers = {'Accept': 'text/tab-separated-values'}

        async with self.client.request(self, method, url, params, headers=headers, raise_for_status=True, **kwargs) as req:
            return pd.read_csv(await req.text(), sep='\t', header=0, index_col=False)


    async def request_json(self, method, url, *, params={}, data=None, json=None):
        headers = {'Accept': 'application/json'}

        async with self.client.request(method, url, params, headers=headers, raise_for_status=True, **kwargs) as req:
            return await req.json()


    async def get_name(self, db, desc):
        headers = {'Accept': 'text/plain'}

        url = URL('/') / self.source_name / 'name'

        async with self.client.request(client, 'GET', url, json=desc) as req:
            return await req.text()
