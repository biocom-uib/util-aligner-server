from aiohttp import ClientSession
from asyncio import gather, get_event_loop
import logging
from json import dumps as json_dumps
from config import config
from mongo import retrieve_file, retrieve_result, insert_result, insert_comparison
import pandas as pd
from os import path
from server_queue import app
import time
from util import write_tsv_to_string

import aligners
from scores import compute_scores, split_score_data_as_tsvs
from sources import IsobaseLocal, StringDB

logger = logging.getLogger(__name__)


@app.task(name='compare_alignments', queue='server_comparer')
def compare_alignments_sync(data):
    loop = get_event_loop()
    return loop.run_until_complete(compare_alignments(data))

async def compare_alignments(data):
    logger.info(f'combining alignments {data}')

    job_id = data['job_id']
    result_ids = data['results_object_ids']

    assert len(result_ids) > 0

    records = await gather(*[retrieve_result(result_id) for result_id in result_ids])
    records = [record for record in records if record['ok'] and alignment_tsv in record['files']]

    db = records[0]['db']
    net1_desc = records[0]['net1']
    net2_desc = records[0]['net2']

    if all(record['db'] == db and record['net1'] == net1_desc and record['net2'] == net2_desc for record in records):
        aligners = [record['aligner'].lower() for record in records]
        assert len(aligners) == len(set(aligners))

        alignments = await gather(*[retrieve_file(record['files']['alignment_tsv']) for record in records])

        alignments = [pd.read_csv(alignment, sep='\t', index_col=0)
                        .rename(columns = lambda n: f"{n}_{record['aligner']}")
                      for record, alignment in zip(records, alignments)]

        joined = pd.concat(alignments, join='outer', axis=1, sort=False)

        def all_equal(xs): return xs.empty or all(xs[0] == x for x in xs)

        intersection = joined.loc[joined.agg(all_equal, axis=1)]

        scores = compute_scores(intersection)

        # joined_alignments: outer join
        # alignment: consens = ø
        # scores: compute_scores(consens)

        result_id = await insert_comparison(job_id, joined)

    else:
        result_id = await insert_comparison(job_id, joined)

    await send_finished_comparison(job_id, result_id)


@app.task(name='process_alignment', queue='server_aligner')
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

    net1 = None
    net2 = None

    try:
        if db_name == 'isobase':
            db = IsobaseLocal('/opt/local-db/isobase')

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

            if net1_net2_scores is None:
                raise ValueError('bitscore matrix not available for the selected network pair')

            if aligner_name == 'alignet':
                net1_scores = await db.get_bitscore_matrix(net1, net1)
                if net1_scores is None:
                    raise ValueError('bitscore matrix not available for the first network')

                net2_scores = await db.get_bitscore_matrix(net2, net2)
                if net2_scores is None:
                    raise ValueError('bitscore matrix not available for the second network')

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

        results = aligner.run(
            *run_args,
            run_dir_base_path='/opt/running-alignments',
            template_dir_base_path='/opt/aligner-templates')

        results['exception'] = None
    except Exception as e:
        logger.exception(f'[{job_id}] exception was raised running alignment')
        results = {'ok': False, 'exception': str(e)}

    response_data = {
        'db': db_name,

        'net1': net1_desc,
        'n_vert1': net1.igraph.vcount() if net1 is not None else -1,
        'n_edges1': net1.igraph.ecount() if net1 is not None else -1,

        'net2': net2_desc,
        'n_vert2': net2.igraph.vcount() if net2 is not None else -1,
        'n_edges2': net2.igraph.ecount() if net2 is not None else -1,

        'aligner': aligner_name,
        'aligner_params': aligner_params,

        'results': results,
        'timestamp': time.time(),
    }

    result_files = dict()

    if 'alignment' in results:
        alignment = results['alignment']

        result_files['alignment_tsv'] = write_tsv_to_string(alignment, header=results['alignment_header'])
        results['alignment'] = None
        del results['alignment_header']

        logger.info(f'[{job_id}] retrieving GO annotations')
        ontology_mapping = await db.get_ontology_mapping([net1, net2])

        logger.info(f'[{job_id}] computing scores')
        response_data['scores'] = compute_scores(net1, net2, alignment, ontology_mapping)
        result_files.update(split_score_data_as_tsvs(response_data['scores']))

    result_id = await insert_result(job_id, response_data, result_files)

    logger.info(f'[{job_id}] finished with result {result_id}')
    logger.debug(f'[{job_id}] job result: {response_data}')

    await send_finished_alignment(job_id, result_id)


async def send_finished_alignment(job_id, result_id):
    url = config['FINISHED_ALIGNMENT_URL']
    return await send_finished_job(job_id, result_id, url)

async def send_finished_comparison(job_id, result_id):
    url = config['FINISHED_COMPARISON_URL']
    return await send_finished_job(job_id, result_id, url)

async def send_finished_job(job_id, result_id, url):
    headers = {'content-type': 'application/json'}
    data = {'job_id': job_id, 'result_id': str(result_id)}

    async with ClientSession(headers=headers) as session:
        async with session.post(url, data=json_dumps(data)) as response:
            response.raise_for_status()

if __name__ == '__main__':
    process_alignment.delay(data={"db": 'Test'})

