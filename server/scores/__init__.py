
from server.util import write_tsv_to_string
from server.scores.topology import compute_ec_scores
from server.scores.functional import compute_fc_scores



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
            parent_dict[path[-1]] = {'file': tsv_key}

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
