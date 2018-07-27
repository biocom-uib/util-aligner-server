from aiohttp import ClientSession
from asyncio import get_event_loop
import logging
from json import dumps as json_dumps
from config import config
from mongo import insert_result
from os import path
from server_queue import app
import time
from util import edgelist_to_tsv

import aligners
from scores import compute_scores, split_score_data_as_tsvs
from sources import IsobaseLocal, StringDB

logger = logging.getLogger(__name__)


@app.task(name='process_alignment', queue='server_default')
def process_alignment_sync(data):
    loop = get_event_loop()
    return loop.run_until_complete(process_alignment(data))

async def process_alignment(data):
    logger.info(f'processing alignment {data}')

    job_id = data['job_id']

    db_name = data['db'].lower()
    net1_desc = data['net1']
    net2_desc = data['net2']
    aligner_name = data['aligner'].lower()
    aligner_params = data.get('aligner_params', dict())

    if db_name == 'isobase':
        db = IsobaseLocal('/opt/networks/isobase')

        net1 = await db.get_network(net1_desc)
        net2 = await db.get_network(net2_desc)
        net1_net2_scores = await db.get_bitscore_matrix(net1, net2)

        if aligner_name == 'alignet':
            net1_scores = await db.get_bitscore_matrix(net1, net1)
            net2_scores = await db.get_bitscore_matrix(net2, net2)
            run_args = (net1, net2, net1_scores, net2_scores, net1_net2_scores)
        else:
            run_args = (net1, net2, net1_net2_scores)

    elif db_name == 'stringdb':
        db = StringDB()
        await db.connect()

        logger.info(f'[{job_id}] fetching data')

        if net1_desc['species_id'] >= 0:
            net1 = await db.get_network(net1_desc['species_id'], score_thresholds=net1_desc['score_thresholds'])
        else:
            net1 = await db.build_custom_network(net1_desc['edges'])

        if net2_desc['species_id'] >= 0:
            net2 = await db.get_network(net2_desc['species_id'], score_thresholds=net2_desc['score_thresholds'])
        else:
            net2 = await db.build_custom_network(net2_desc['edges'])

        net1_net2_scores = await db.get_bitscore_matrix(net1, net2)

        if aligner_name == 'alignet':
            net1_scores = await db.get_bitscore_matrix(net1, net1)
            net2_scores = await db.get_bitscore_matrix(net2, net2)
            run_args = (net1, net2, net1_scores, net2_scores, net1_net2_scores)
        else:
            run_args = (net1, net2, net1_net2_scores)

    else:
        raise ValueError(f'[{job_id}] database not supported: {db_name}')

    aligners_dispatcher = {
        'alignet':  aligners.Alignet,
        'hubalign': aligners.Hubalign,
        'l-graal':  aligners.LGraal,
        'pinalog':  aligners.Pinalog,
        'spinal':   aligners.Spinal,
    }

    if aligner_name not in aligners_dispatcher:
        raise ValueError(f'[{job_id}] aligner not supported: {aligner_name}')

    aligner = aligners_dispatcher[aligner_name](**aligner_params)

    try:
        results = aligner.run(
            *run_args,
            run_dir_base_path='/opt/running-alignments',
            template_dir_base_path='/opt/aligner-templates')
    except:
        logger.exception(f'[{job_id}] exception was raised running alignment')
        results = {'ok': False}

    response_data = {
        'db': db_name,

        'net1': net1_desc,
        'n_vert1': net1.igraph.vcount(),
        'n_edges1': net1.igraph.ecount(),

        'net2': net2_desc,
        'n_vert2': net2.igraph.vcount(),
        'n_edges2': net2.igraph.ecount(),

        'aligner': aligner_name,
        'aligner_params': aligner_params,

        'results': results,
        'timestamp': time.time(),
    }

    result_files = dict()

    if 'alignment' in results:
        alignment = results['alignment']

        result_files['alignment_tsv'] = edgelist_to_tsv(alignment, header=results['alignment_header'])
        results['alignment'] = None
        del results['alignment_header']

        ontology_mapping = await db.get_ontology_mapping([net1, net2])
        response_data['scores'] = compute_scores(net1, net2, alignment, ontology_mapping)
        result_files.update(split_score_data_as_tsvs(response_data['scores']))

    result_id = await insert_result(job_id, response_data, result_files)

    logger.info(f'[{job_id}] finished with result {result_id}')
    logger.debug(f'[{job_id}] job result: {response_data}')

    await send_finished_job(job_id, result_id)


async def send_finished_job(job_id, result_id):
    headers = {'content-type': 'application/json'}
    data = {'job_id': job_id, 'result_id': str(result_id)}

    url = config['FINISHED_JOB_URL']
    async with ClientSession(headers=headers) as session:
        async with session.post(url, data=json_dumps(data)) as response:
            response.raise_for_status()

if __name__ == '__main__':
    process_alignment.delay(data={"db": 'Test'})

