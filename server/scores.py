import itertools


def compute_ec(net1, net2, alignment):
    net1 = net1.igraph
    net2 = net2.igraph
    alignment = dict(alignment)

    unknown_edges_net1 = set()
    unknown_nodes_net1 = set()
    unknown_nodes_net2 = set()

    num_preserved_edges = 0
    broken_edges = set()

    for e in net1.es:
        p1_id, p2_id = e.tuple
        p1_name, p2_name = net1.vs[p1_id]['name'], net1.vs[p2_id]['name']

        if p1_name is None or p2_name is None:
            sys.stderr.write(f'error: missing edge: {e}\n')
            continue

        fp1_name, fp2_name = alignment.get(p1_name), alignment.get(p2_name)
        if fp1_name is None:
            unknown_nodes_net1.add(p1_name)
            unknown_edges_net1.add((p1_name, p2_name))
            continue
        if fp2_name is None:
            unknown_nodes_net1.add(p2_name)
            unknown_edges_net1.add((p1_name, p2_name))
            continue

        fp1s, fp2s = net2.vs.select(name=fp1_name), net2.vs.select(name=fp2_name)

        if len(fp1s) == 0:
            unknown_nodes_net2.add(fp1_name)
            continue
        if len(fp2s) == 0:
            unknown_nodes_net2.add(fp2_name)
            continue

        fp1_id, fp2_id = fp1s[0].index, fp2s[0].index

        if net2.get_eid(fp1_id, fp2_id, directed=False, error=False) >= 0:
            num_preserved_edges += 1
        else:
            broken_edges.add((p1_name, p2_name))

    min_es = net1.ecount() if net1.vcount() <= net2.vcount() else net2.ecount()

    return {
        'n_edges1': net1.ecount(),
        'n_edges2': net2.ecount(),
        'n_vert1': net1.vcount(),
        'n_vert2': net2.vcount(),
        'min_n_edges': min_es,
        'unknown_edges_net1': list(unknown_edges_net1),
        'unknown_nodes_net1': list(unknown_nodes_net1),
        'unknown_nodes_net2': list(unknown_nodes_net2),
        'num_preserved_edges': num_preserved_edges,
        'broken_edges': list(broken_edges),
        'ec_score': num_preserved_edges / min_es,
    }


def compute_fc(net1, net2, alignment, ontology_mapping):
    fc_sum = 0
    fc_len = 0

    no_go_prots_net1 = set()
    no_go_prots_net2 = set()

    for p1_name, p2_name in alignment:
        gos1 = frozenset(ontology_mapping.get(p1_name, []))
        gos2 = frozenset(ontology_mapping.get(p2_name, []))

        if not gos1:
            no_go_prots_net1.add(p1_name)
        if not gos2:
            no_go_prots_net2.add(p2_name)

        len_union = len(gos1.union(gos2))

        if len_union > 0:
            len_intersection = len(gos1.intersection(gos2))
            fc_sum += len_intersection / len_union
            fc_len += 1

    return {
        'fc_score': fc_sum/fc_len,
        'unannotated_prots_net1': list(no_go_prots_net1),
        'unannotated_prots_net2': list(no_go_prots_net2),
    }


def compute_scores(net1, net2, result, ontology_mapping):
    alignment = result['alignment']

    ec_data = compute_ec(net1, net2, alignment)
    fc_data = compute_fc(net1, net2, alignment, ontology_mapping)

    return {
        'ec_data': ec_data,
        'fc_data': fc_data
    }
