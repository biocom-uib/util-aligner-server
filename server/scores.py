from itertools import chain
from collections import Counter

def compute_ec(net1, net2, alignment):
    net1 = net1.igraph
    net2 = net2.igraph
    min_es = net1.ecount() if net1.vcount() <= net2.vcount() else net2.ecount()

    result = {
        'min_n_edges': min_es
    }

    alignment = dict(alignment)

    unaligned_edges_net1 = set()
    unaligned_nodes_net1 = set()
    unknown_nodes_net2 = set()

    # compute EC and non_preserved_edges

    num_preserved_edges = 0
    non_preserved_edges = set()

    for e in net1.es:
        p1_id, p2_id = e.tuple
        p1_name, p2_name = net1.vs[p1_id]['name'], net1.vs[p2_id]['name']

        fp1_name, fp2_name = alignment.get(p1_name), alignment.get(p2_name)
        if fp1_name is None:
            unaligned_nodes_net1.add(p1_name)
            unaligned_edges_net1.add((p1_name, p2_name))
            continue
        if fp2_name is None:
            unaligned_nodes_net1.add(p2_name)
            unaligned_edges_net1.add((p1_name, p2_name))
            continue

        fp1s, fp2s = net2.vs.select(name=fp1_name), net2.vs.select(name=fp2_name)

        if len(fp1s) == 0:
            unknown_nodes_net2.add(fp1_name)
            continue
        if len(fp2s) == 0:
            unknown_nodes_net2.add(fp2_name)
            continue

        if net2.get_eid(fp1s[0].index, fp2s[0].index, directed=False, error=False) >= 0:
            num_preserved_edges += 1
        else:
            non_preserved_edges.add((p1_name, p2_name))

    def node_preimage(p_name):
        selections = (net1.vs.select(name=preim_name) for preim_name, value in alignment.items() if value == p_name)
        return [v.index for v in chain.from_iterable(selections)]

    # compute non_reflected_edges

    non_reflected_edges = set()

    for e in net2.es:
        p1_id, p2_id = e.tuple
        p1_name, p2_name = net2.vs[p1_id]['name'], net2.vs[p2_id]['name']

        preim_p1_ids = node_preimage(p1_name)
        preim_p2_ids = node_preimage(p2_name)

        if all(net1.get_eid(preim_p1_id, preim_p2_id, directed=False, error=False) < 0
                for preim_p1_id in preim_p1_ids
                for preim_p2_id in preim_p2_ids):
            non_reflected_edges.add((p1_name, p2_name))

    result.update({
        'unaligned_edges_net1': list(unaligned_edges_net1),
        'unaligned_nodes_net1': list(unaligned_nodes_net1),
        'unknown_nodes_net2': list(unknown_nodes_net2),
        'non_preserved_edges': list(non_preserved_edges),
        'non_reflected_edges': list(non_reflected_edges),
        'num_preserved_edges': num_preserved_edges,
        'ec_score': num_preserved_edges / min_es,
    })

    return result

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


def compute_fc(net1, net2, alignment, ontology_mapping):
    fc_sum = 0
    fc_len = 0

    for p1_name, p2_name in alignment:
        gos1 = frozenset(ontology_mapping.get(p1_name, []))
        gos2 = frozenset(ontology_mapping.get(p2_name, []))

        len_union = len(gos1.union(gos2))

        if len_union > 0:
            len_intersection = len(gos1.intersection(gos2))
            fc_sum += len_intersection / len_union
            fc_len += 1

    ann_freqs_net1, no_go_prots_net1 = count_annotations(net1, ontology_mapping)
    ann_freqs_net2, no_go_prots_net2 = count_annotations(net2, ontology_mapping)

    return {
        'fc_score': fc_sum/fc_len,
        'unannotated_prots_net1': list(no_go_prots_net1),
        'unannotated_prots_net2': list(no_go_prots_net2),
        'ann_freqs_net1': {str(ann_cnt): freq for ann_cnt, freq in ann_freqs_net1.items()},
        'ann_freqs_net2': {str(ann_cnt): freq for ann_cnt, freq in ann_freqs_net2.items()}
    }


def compute_scores(net1, net2, result, ontology_mapping):
    alignment = result['alignment']

    ec_data = compute_ec(net1, net2, alignment)
    fc_data = compute_fc(net1, net2, alignment, ontology_mapping)

    return {
        'ec_data': ec_data,
        'fc_data': fc_data
    }
