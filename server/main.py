from aiohttp import ClientSession
from asyncio import get_event_loop
import logging
from json import dumps as json_dumps
from config import config
from server_queue import app


logger = logging.getLogger(__name__)


@app.task(name='process_alignment', queue='server_default')
def process_alignment(data):
    logger.info(data)
    logger.info(data['db'])
    send_finished_job(data['job_id'])


def send_finished_job(job_id):
    loop = get_event_loop()
    loop.run_until_complete(_send_finished_job(job_id))


async def _send_finished_job(job_id):
    headers = {'content-type': 'application/json'}
    data = {'job_id': job_id}
    url = config['FINISHED_JOB_URL']
    async with ClientSession(headers=headers) as session:
        async with session.post(url, data=json_dumps(data)) as response:
            response.raise_for_status()

if __name__ == '__main__':
    process_alignment.delay(data={"db": 'Test'})
