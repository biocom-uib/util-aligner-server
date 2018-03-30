import logging
from server_queue import app

logger = logging.getLogger(__name__)


@app.task(name='process_alignment', queue='server_default')
def process_alignment(data):
    logger.info(data)
    logger.info(data['db'])


if __name__ == '__main__':
    process_alignment.delay(data={"db": 'Test'})
