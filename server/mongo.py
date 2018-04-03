import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://mongo:27017')
db = client.util_aligner


async def insert_result(results):
    response = await db.results.insert_one(results)
    return response.inserted_id
