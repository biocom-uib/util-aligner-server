from bson.objectid import ObjectId
import motor.motor_asyncio
import ujson

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://util-aligner-server:util-aligner-server@mongo:27017/util_aligner')
db = client.util_aligner

gridfs = motor.motor_asyncio.AsyncIOMotorGridFSBucket(db)


async def insert_comparison(job_id, joined_alignment):
    pass

async def retrieve_file(file_id):
    f = await gridfs.open_download_stream(ObjectId(file_id))
    return await f.read()

async def retrieve_result(insert_id):
    return await gridfs.results.find_one({'_id': ObjectId(insert_id)})


async def insert_result(job_id, results, files=dict()):
    file_ids = dict()

    for filename, content in files.items():
        file_ids[filename] = await gridfs.upload_from_stream(f'{job_id}_{filename}', content.encode('utf-8'))

    results['files'] = file_ids

    result_id = await db.results.insert_one(results)
    return result_id.inserted_id
