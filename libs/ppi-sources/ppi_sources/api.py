from aiohttp import ClientTimeout, ClientResponseError
from contextlib import asynccontextmanager
from io import StringIO
from msgpack import unpackb
import pandas as pd
from yarl import URL



class SourceAPIClient(object):
    NO_TIMEOUT = ClientTimeout()
    DEFAULT_TIMEOUT = ClientTimeout()

    def __init__(self, http_client, host, source_name):
        self.client = http_client
        self.host = URL(host)
        self.source_name = source_name

    def complete_url(self, path):
        return self.host.with_path(path)


    @asynccontextmanager
    async def request(self, method, path, **kwargs):
        url = self.complete_url(path)

        try:
            async with self.client.request(method, url, **kwargs) as resp:
                yield resp
        except ClientResponseError as exc:
            print('HTTP error:', exc.status, exc.message)
            raise


    async def request_msgpack(self, method, path, **kwargs):
        headers = {'Accept': 'application/x-msgpack'}

        async with self.request(method, path, headers=headers, raise_for_status=True, **kwargs) as resp:
            return unpackb(await resp.read(), raw=False)


    async def request_tsv(self, method, path, **kwargs):
        headers = {'Accept': 'text/tab-separated-values'}

        async with self.request(method, path, headers=headers, raise_for_status=True, **kwargs) as resp:
            return pd.read_csv(StringIO(await resp.text()), sep='\t', header=0, index_col=False)


    async def request_dataframe(self, method, path, **kwargs):
        return pd.DataFrame(await self.request_msgpack(method, path, **kwargs))


    async def request_json(self, method, path, **kwargs):
        headers = {'Accept': 'application/json'}

        async with self.request(method, path, headers=headers, raise_for_status=True, **kwargs) as resp:
            return await resp.json()


    async def get_name(self, desc):
        headers = {'Accept': 'text/plain'}

        path = URL('/') / self.source_name / 'name'

        async with self.request(client, 'GET', path, json=desc) as resp:
            return await resp.text()
