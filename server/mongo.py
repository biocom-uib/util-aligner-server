import motor.motor_asyncio
import ujson

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://util-aligner-server:util-aligner-server@mongo:27017/util_aligner')
db = client.util_aligner

gridfs = motor.motor_asyncio.AsyncIOMotorGridFSBucket(db)


async def insert_result(filename, results):
    file_id = await gridfs.upload_from_stream(filename, ujson.dumps(results))
    return file_id
