from collections import Counter
from itertools import chain
from math import isnan
import pandas as pd

from go_tools import init_default_hrss
import geneontology as godb
import semantic_similarity as semsim

from server.util import write_tsv_to_string


def map_column_alignment(df, alignment_series, column):
    return df.join(alignment_series.rename(column), on=column, how='left', lsuffix='_orig')

def reverse_alignment_series(alignment_series):
    return alignment_series.reset_index().set_index(alignment_series.name).iloc[:, 0]


def get_alignment_image(net_df, alignment_series):
    src, tgt = net_df.columns[:2]

    return net_df \
        .pipe(map_column_alignment, alignment_series, src) \
        .pipe(map_column_alignment, alignment_series, tgt)


def add_is_edge_column(image, net_igraph):
    if image.empty:
        return image.assign(is_edge = pd.Series([], dtype=bool))

    return image \
        .assign(is_edge =
            image.apply(lambda row:
                    pd.notna(row.source) and
                    pd.notna(row.target) and
                    net_igraph[row.source, row.target] > 0,
                axis='columns'))


def compute_ec_image_scores(net1, net2, alignment):
    alignment_series = alignment.iloc[:, 0]

    # restrict the alignment to the given networks
    valid_source_ix = alignment_series.index.to_series().isin(frozenset(net1.igraph.vs['name'])).to_numpy()
    valid_image_ix = alignment_series.isin(frozenset(net2.igraph.vs['name'])).to_numpy()

    alignment_series = alignment_series.loc[valid_source_ix & valid_image_ix]

    invalid_images = alignment.iloc[:, 0].loc[valid_source_ix & ~valid_image_ix].unique().tolist()

    unaligned_nodes = list(frozenset(net1.igraph.vs['name']) - frozenset(alignment_series.index.to_numpy()))

    image = get_alignment_image(net1.to_dataframe(), alignment_series) \
        .pipe(add_is_edge_column, net2.igraph)

    preserved_edges = image.loc[:, ['source_orig', 'target_orig', 'is_edge']] \
        .groupby(['source_orig', 'target_orig']) \
        .all()

    num_preserved_edges = int(preserved_edges.is_edge.sum())

    non_preserved_edges = list(
        preserved_edges.reset_index()
            .loc[lambda df: ~df.is_edge, ['source_orig', 'target_orig']]
            .astype(str)
            .itertuples(index=False, name=None)
    )

    unaligned_edges = list(
        image.loc[image.source.isna() | image.target.isna(), ['source_orig', 'target_orig']]
            .astype(str)
            .itertuples(index=False, name=None)
    )

    min_es = net1.igraph.ecount() if net1.igraph.vcount() <= net2.igraph.vcount() else net2.igraph.ecount()

    return {
        'invalid_images': invalid_images,
        'num_invalid_images': len(invalid_images),

        'unaligned_nodes': unaligned_nodes,
        'num_unaligned_nodes': len(unaligned_nodes),

        'unaligned_edges': unaligned_edges,
        'num_unaligned_edges': len(unaligned_edges),

        'num_preserved_edges': num_preserved_edges,
        'non_preserved_edges': non_preserved_edges,

        'min_n_edges': min_es,
        'ec_score': num_preserved_edges/min_es if min_es > 0 else -1.0,
    }


def compute_ec_preimage_scores(net1, net2, alignment):
    alignment_series = reverse_alignment_series(alignment.iloc[:, 0])
    valid_preimage_ix = alignment_series.isin(frozenset(net1.igraph.vs['name'])).to_numpy()
    alignment_series = alignment_series.loc[valid_preimage_ix]

    # we dropna() here, but not in compute_ec_image_scores

    preimage = get_alignment_image(net2.to_dataframe(), alignment_series) \
        .dropna() \
        .pipe(add_is_edge_column, net1.igraph)

    reflected_edges = preimage.loc[:, ['source_orig', 'target_orig', 'is_edge']] \
        .groupby(['source_orig', 'target_orig']) \
        .all()

    num_reflected_edges = int(reflected_edges.is_edge.sum())

    non_reflected_edges = list(
        reflected_edges.reset_index() \
            .loc[lambda df: ~df.is_edge, ['source_orig', 'target_orig']] \
            .astype(str)
            .itertuples(name=None)
    )

    return {
        'num_reflected_edges': num_reflected_edges,
        'non_reflected_edges': non_reflected_edges
    }


def compute_ec_scores(net1, net2, alignment):
    try:
        scores = {}
        scores.update(compute_ec_image_scores(net1, net2, alignment))
        scores.update(compute_ec_preimage_scores(net1, net2, alignment))
    except:
        print(net1, net2, alignment)
        raise

    if isinstance(net1, VirusHostNetwork) and isinstance(net2, VirusHostNetwork):
        scores.update({
            'host_ec_data':         compute_ec_scores(net1.host_net, net2.host_net, alignment),
            'virus_ec_data':        compute_ec_scores(net1.virus_net, net2.virus_net, alignment),
            'vh_bipartite_ec_data': compute_ec_scores(net1.vh_bipartite_net, net2.vh_bipartite_net, alignment)
        })

    return scores


def count_annotations(net, ontology_mapping):
    ann_freqs = Counter()
    no_go_prots = set()

    for p in net.igraph.vs:
        p_name = p['name']
        gos = frozenset(ontology_mapping.get(p_name, []))

        ann_freqs[len(gos)] += 1

        if not gos:
            no_go_prots.add(p_name)

    return ann_freqs, no_go_prots


def compute_fc(alignment, ontology_mapping, dissim):
    fc_sum = 0
    fc_len = 0

    results = []

    for p1_name, p2_name in alignment.itertuples(name=None):
        gos1 = frozenset(ontology_mapping.get(p1_name, []))
        gos2 = frozenset(ontology_mapping.get(p2_name, []))

        fc = dissim(gos1, gos2)

        if not isnan(fc):
            results.append((p1_name, p2_name, fc))
            fc_sum += fc
            fc_len += 1

    fc_avg = fc_sum/fc_len if fc_len > 0 else -1
    return results, fc_avg


def compute_bitscore_fc(alignment, bitscore_matrix):
    bitscore_df = bitscore_matrix.to_dataframe()

    alignment_df = alignment.reset_index()
    alignment_df.columns = bitscore_df.index.names

    relevant_bitscores = bitscore_df.join(alignment_df.set_index(bitscore_df.index.names), how='inner')

    return float(relevant_bitscores.bitscore.sum() / bitscore_df.bitscore.max()) \
        if not relevant_bitscores.empty else -1


jaccard_dissim = semsim.JaccardSim().compare
hrss_bma_sim = init_default_hrss().compare

def compute_fc_scores(net1, net2, alignment, bitscore_matrix, ontology_mapping):
    bitscore_fc = compute_bitscore_fc(alignment, bitscore_matrix)

    fc_data = {'fc_score_bitscore': bitscore_fc}

    if ontology_mapping:
        fc_values_jaccard,  fc_jaccard  = compute_fc(alignment, ontology_mapping, jaccard_dissim)
        fc_values_hrss_bma, fc_hrss_bma = compute_fc(alignment, ontology_mapping, hrss_bma_sim)

        ann_freqs_net1, no_go_prots_net1 = count_annotations(net1, ontology_mapping)
        ann_freqs_net2, no_go_prots_net2 = count_annotations(net2, ontology_mapping)

        fc_data.update({
            'fc_score_jaccard': fc_jaccard,
            'fc_values_jaccard': fc_values_jaccard,
            'fc_score_hrss_bma': fc_hrss_bma,
            'fc_values_hrss_bma': fc_values_hrss_bma,
            'unannotated_prots_net1': list(no_go_prots_net1),
            'unannotated_prots_net2': list(no_go_prots_net2),
            'ann_freqs_net1': {str(ann_cnt): freq for ann_cnt, freq in ann_freqs_net1.items()},
            'ann_freqs_net2': {str(ann_cnt): freq for ann_cnt, freq in ann_freqs_net2.items()}
        })

    return fc_data


def compute_scores(net1, net2, alignment, bitscore_matrix, ontology_mapping):
    ec_data = compute_ec_scores(net1, net2, alignment)
    fc_data = compute_fc_scores(net1, net2, alignment, bitscore_matrix, ontology_mapping)

    return {
        'ec_data': ec_data,
        'fc_data': fc_data
    }


def split_score_data_as_tsvs(scores):
    tsvs = dict()

    def navigate(d, path):
        for key in path[:-1]:
            if key in d:
                d = d[key]
            else:
                return None

        return d

    def key_to_file(*path):
        parent_dict = navigate(scores, path)

        if parent_dict is not None and path[-1] in parent_dict:
            tsv_key = '/'.join(path) + '_tsv'

            tsvs[tsv_key] = write_tsv_to_string(parent_dict[path[-1]])
            parent_dict[path[-1]] = None

    key_to_file('ec_data', 'invalid_images')
    key_to_file('ec_data', 'unaligned_nodes')
    key_to_file('ec_data', 'unaligned_edges')
    key_to_file('ec_data', 'non_preserved_edges')
    key_to_file('ec_data', 'non_reflected_edges')

    key_to_file('ec_data', 'host_ec_data', 'invalid_images')
    key_to_file('ec_data', 'host_ec_data', 'unaligned_nodes')
    key_to_file('ec_data', 'host_ec_data', 'unaligned_edges')
    key_to_file('ec_data', 'host_ec_data', 'non_preserved_edges')
    key_to_file('ec_data', 'host_ec_data', 'non_reflected_edges')

    key_to_file('ec_data', 'virus_ec_data', 'invalid_images')
    key_to_file('ec_data', 'virus_ec_data', 'unaligned_nodes')
    key_to_file('ec_data', 'virus_ec_data', 'unaligned_edges')
    key_to_file('ec_data', 'virus_ec_data', 'non_preserved_edges')
    key_to_file('ec_data', 'virus_ec_data', 'non_reflected_edges')

    key_to_file('ec_data', 'vh_bipartite_ec_data', 'invalid_images')
    key_to_file('ec_data', 'vh_bipartite_ec_data', 'unaligned_nodes')
    key_to_file('ec_data', 'vh_bipartite_ec_data', 'unaligned_edges')
    key_to_file('ec_data', 'vh_bipartite_ec_data', 'non_preserved_edges')
    key_to_file('ec_data', 'vh_bipartite_ec_data', 'non_reflected_edges')

    key_to_file('fc_data', 'fc_values_jaccard')
    key_to_file('fc_data', 'fc_values_hrss_bma')
    key_to_file('fc_data', 'unannotated_prots_net1')
    key_to_file('fc_data', 'unannotated_prots_net2')

    return tsvs
