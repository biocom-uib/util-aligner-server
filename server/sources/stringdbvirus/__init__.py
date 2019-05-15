from contextlib import asynccontextmanager

from server.sources.stringdbvirus.local import StringDBVirusLocalSource


@asynccontextmanager
async def stringdbvirus_local_source(base_path):
    yield StringDBVirusLocal(base_path)
