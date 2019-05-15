from bson.objectid import ObjectId
import motor.motor_asyncio
import ujson

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://util-aligner-server:util-aligner-server@mongo:27017/util_aligner')
db = client.util_aligner

gridfs = motor.motor_asyncio.AsyncIOMotorGridFSBucket(db)



async def retrieve_file(file_id):
    f = await gridfs.open_download_stream(ObjectId(file_id))
    return await f.read()

async def retrieve_alignment_result(insert_id):
    return await db.alignments.find_one({'_id': ObjectId(insert_id)})


async def insert_split(collection, job_id, document, files=dict()):
    file_ids = dict()

    for filename, content in files.items():
        file_id = await gridfs.upload_from_stream(f'{job_id}/{filename}', content.encode('utf-8'))
        file_ids[filename] = str(file_id)

    document['files'] = file_ids

    result_id = await collection.insert_one(document)
    return result_id.inserted_id

async def insert_alignment(job_id, response_data, files=dict()):
    return await insert_split(db.alignments, job_id, response_data, files)

async def insert_comparison(job_id, response_data, files=dict()):
    return await insert_split(db.comparisons, job_id, response_data, files)
