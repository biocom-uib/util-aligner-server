from aiohttp import ClientSession
from asyncio import get_event_loop
import logging
from json import dumps as json_dumps
from config import config
from mongo import insert_result
from os import path
from server_queue import app
import time

import aligners
from scores import compute_scores
from sources import IsobaseLocal

logger = logging.getLogger(__name__)


def insert_result_sync(results):
    loop = get_event_loop()
    return loop.run_until_complete(insert_result(results))


@app.task(name='process_alignment', queue='server_default')
def process_alignment(data):
    logger.info(f'processing alignment {data}')

    job_id = data['job_id']

    db_name = data['db'].lower()
    net1_name = data['net1']
    net2_name = data['net2']
    aligner_name = data['aligner'].lower()
    aligner_params = data.get('aligner_params', dict())

    if db_name == 'isobase':
        db = IsobaseLocal('/opt/networks/isobase')
        net1 = db.get_network(net1_name)
        net2 = db.get_network(net2_name)
        net1_net2_scores = db.get_bitscore_matrix(net1_name, net2_name, net1=net1, net2=net2)

        if aligner_name == 'alignet':
            net1_scores = db.get_bitscore_matrix(net1_name)
            net2_scores = db.get_bitscore_matrix(net2_name)
            run_args = (net1, net2, net1_scores, net2_scores, net1_net2_scores)
        else:
            run_args = (net1, net2, net1_net2_scores)
    else:
        raise ValueError(f'database not supported: {db_name}')

    aligners_dispatcher = {
        'alignet':  aligners.Alignet,
        'hubalign': aligners.Hubalign,
        'l-graal':  aligners.LGraal,
        'pinalog':  aligners.Pinalog,
        'spinal':   aligners.Spinal,
    }

    if aligner_name not in aligners_dispatcher:
        raise ValueError(f'aligner not supported: {aligner_name}')

    aligner = aligners_dispatcher[aligner_name](**aligner_params)

    try:
        results = aligner.run(
            *run_args,
            run_dir_base_path='/opt/running-alignments',
            template_dir_base_path='/opt/aligner-templates')
    except:
        logger.exception('exception was raised running alignment')
        results = {'ok': False}

    response_data = {
        'db': db_name,

        'net1': net1_name,
        'n_vert1': net1.igraph.vcount(),
        'n_edges1': net1.igraph.ecount(),

        'net2': net2_name,
        'n_vert2': net2.igraph.vcount(),
        'n_edges2': net2.igraph.ecount(),

        'aligner': aligner_name,
        'aligner_params': aligner_params,

        'results': results,
        'timestamp': time.time(),
    }

    if 'alignment' in results:
        response_data['scores'] = compute_scores(net1, net2, results, db.get_ontology_mapping([net1,net2]))

    result_id = insert_result_sync(response_data)

    logger.info(f'job {job_id} finished with result {result_id}')
    logger.debug(f'job result: {response_data}')

    send_finished_job(job_id, result_id)


def send_finished_job(job_id, result_id):
    loop = get_event_loop()
    loop.run_until_complete(_send_finished_job(job_id, result_id))


async def _send_finished_job(job_id, result_id):
    headers = {'content-type': 'application/json'}
    data = {'job_id': job_id, 'result_id': str(result_id)}

    url = config['FINISHED_JOB_URL']
    async with ClientSession(headers=headers) as session:
        async with session.post(url, data=json_dumps(data)) as response:
            response.raise_for_status()

if __name__ == '__main__':
    process_alignment.delay(data={"db": 'Test'})

