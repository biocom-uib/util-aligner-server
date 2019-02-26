import os
import pandas as pd
from os import path
import shutil
import subprocess

import stringdb_virus
import get_seqs


SCRIPT_DIR = path.dirname(__file__)

NETWORKS_BASE_DIR = path.join(SCRIPT_DIR, 'networks')
BLAST_BASE_DIR = path.join(SCRIPT_DIR, 'blast')

ALL_PROTEINS_PATH = path.join(SCRIPT_DIR, 'all_proteins.tsv')
ALL_NETWORKS_PATH = path.join(SCRIPT_DIR, 'all_networks.tsv')
ALL_SEQS_TSV_PATH = path.join(SCRIPT_DIR, 'seqs.tsv')

NCBI_BLAST_HOME = os.getenv('BLAST_HOME')
MAKEBLASTDB_BIN = path.join(NCBI_BLAST_HOME, 'bin', 'makeblastdb')
BLASTP_BIN = path.join(NCBI_BLAST_HOME, 'bin', 'blastp')


def get_virus_host_networks(cursor, host_id, score_types, min_interaction_count):
    networks_dir = path.join(NETWORKS_BASE_DIR, str(host_id))
    os.makedirs(networks_dir, exist_ok=True)

    virus_ids = stringdb_virus.get_viruses_for_host(cursor, host_id, score_types)

    filter_q15 = lambda df: df.evidence_score >= df.evidence_score.quantile(0.15)
    edge_cols = ['node_id_a', 'node_id_b']

    all_prots = set()
    networks = pd.DataFrame(columns = ['virus_id', 'path'])

    for virus_id in virus_ids:
        print(f'fetching network {host_id} - {virus_id}... ', end='')

        network = stringdb_virus.get_virus_host_network(cursor, host_id, virus_id, score_types)

        if network.empty:
            print(f'empty (skipped)')
            continue

        final_network = network \
            .groupby('score_type', as_index=False) \
            .apply(lambda grp: grp.loc[filter_q15]) \
            .pipe(lambda net: stringdb_virus.network_with_external_ids(cursor, net)) \
            .drop_duplicates(subset=edge_cols)

        if len(final_network) >= min_interaction_count:
            print(f'{len(final_network)} edges')

            all_prots.update(final_network.node_id_a)
            all_prots.update(final_network.node_id_b)

            network_path = path.join(networks_dir, f'{host_id}-{virus_id}.tsv')
            networks = networks.append({'virus_id': virus_id, 'path': network_path}, ignore_index=True)

            final_network.loc[:, edge_cols] \
                .to_csv(network_path, sep='\t', index=False, header=True)
        else:
            print(f'{len(final_network)} edges (skipped)')

    pd.DataFrame({'external_id': list(all_prots)}) \
        .to_csv(ALL_PROTEINS_PATH, sep='\t', index=False, header=True)

    networks \
        .to_csv(ALL_NETWORKS_PATH, sep='\t', index=False, header=True)


def get_sequences(cursor, prots):
    seqs = get_seqs.get_seqs(cursor, frozenset(prots.external_id))

    seqs.to_csv(ALL_SEQS_TSV_PATH, sep='\t', index=False, header=True)


def generate_bitscore_matrix(base_dir, name1, seqs1, name2, seqs2):
    assert path.isfile(MAKEBLASTDB_BIN) and path.isfile(BLASTP_BIN)

    pairing_name = f'{name1}-{name2}'
    print(f"running blastp on {pairing_name}")

    work_dir = path.join(base_dir, pairing_name)
    os.makedirs(work_dir)

    with open(path.join(work_dir, f'{name1}.fasta'), 'w+') as f:
        get_seqs.write_seqs_fasta(seqs1, f)

    with open(path.join(work_dir, f'{name2}.fasta'), 'w+') as f:
        get_seqs.write_seqs_fasta(seqs2, f)

    # subprocess.run(
    #     [MAKEBLASTDB_BIN,
    #         '-in', f'{name2}.fasta',
    #         '-input_type', 'fasta',
    #         '-out', '{name2}.blastdb',
    #         '-dbtype', 'prot'],
    #     check = True,
    #     cwd = work_dir)

    output_name = f'{pairing_name}.blast.tsv'

    prot_cols = ['qseqid', 'sseqid']
    blast_cols = ['ppos', 'pident', 'bitscore']

    subprocess.run(
        [BLASTP_BIN,
            '-query', f'{name1}.fasta',
            '-subject', f'{name2}.fasta',
            '-outfmt', '6 ' + ' '.join(prot_cols + blast_cols),
            '-out', output_name,
            '-max_target_seqs', str(len(seqs2))],
        check = True,
        cwd = work_dir)

    tricol = pd.read_csv(
        path.join(work_dir, output_name),
        sep='\t', header=None, names=prot_cols+blast_cols)

    for blast_col in blast_cols:
        # remove duplicate results
        tricol.loc[:, prot_cols + [blast_col]] \
            .groupby(prot_cols, as_index=False) \
            .max() \
            .to_csv(
                path.join(base_dir, f'{pairing_name}.{blast_col}.tsv'),
                sep='\t', header=True, index=False)

    shutil.rmtree(work_dir)

    return tricol


def generate_bitscore_matrices(host_id, nets, seqs):
    base_dir = path.join(BLAST_BASE_DIR, f'{host_id}-{host_id}')
    os.makedirs(base_dir, exist_ok=True)

    seqs = seqs.set_index('external_id')

    for net1 in nets.itertuples():
        net1_edges = pd.read_csv(net1.path, sep='\t', header=0)
        prots1 = list(frozenset(net1_edges.node_id_a) | frozenset(net1_edges.node_id_b))
        seqs1 = seqs.loc[prots1].reset_index()

        for net2 in nets.itertuples():
            if net2.virus_id > net1.virus_id:
                continue

            net2_edges = pd.read_csv(net2.path, sep='\t', header=0)
            prots2 = list(frozenset(net2_edges.node_id_a) | frozenset(net2_edges.node_id_b))
            seqs2 = seqs.loc[prots2].reset_index()

            generate_bitscore_matrix(base_dir, str(net1.virus_id), seqs1, str(net2.virus_id), seqs2)



if __name__ == '__main__':
    host_id = 9606
    score_types = [8, 10]
    min_interaction_count = 30

    conn = stringdb_virus.connect_to_docker()
    cursor = conn.cursor()

    print('fetching networks...')
    # get_virus_host_networks(cursor, host_id, score_types, min_interaction_count)
    nets = pd.read_csv(ALL_NETWORKS_PATH, sep='\t', header=0)
    prots = pd.read_csv(ALL_PROTEINS_PATH, sep='\t', header=0)

    print('searching sequences...')
    # get_sequences(cursor, prots)
    seqs = pd.read_csv(ALL_SEQS_TSV_PATH, sep='\t', header=0)

    print('running BLAST...')
    # generate_bitscore_matrices(host_id, nets, seqs)
