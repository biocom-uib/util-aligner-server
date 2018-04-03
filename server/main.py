from aiohttp import ClientSession
from asyncio import get_event_loop
import logging
from json import dumps as json_dumps
from config import config
from mongo import insert_result
from server_queue import app


logger = logging.getLogger(__name__)


def insert_result_sync(results):
    loop = get_event_loop()
    return loop.run_until_complete(insert_result(results))


@app.task(name='process_alignment', queue='server_default')
def process_alignment(data):
    logger.info(data)
    logger.info(data['db'])
    results = {'results': data}
    result_id = insert_result_sync(results)
    send_finished_job(data['job_id'], result_id)


def send_finished_job(job_id, result_id):
    loop = get_event_loop()
    loop.run_until_complete(_send_finished_job(job_id, result_id))


async def _send_finished_job(job_id, result_id):
    headers = {'content-type': 'application/json'}
    data = {'job_id': job_id, 'result_id': str(result_id)}
    print(data)
    url = config['FINISHED_JOB_URL']
    async with ClientSession(headers=headers) as session:
        async with session.post(url, data=json_dumps(data)) as response:
            response.raise_for_status()

if __name__ == '__main__':
    process_alignment.delay(data={"db": 'Test'})
