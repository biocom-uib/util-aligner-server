import csv
import igraph
import json
from scipy.optimize import linear_sum_assignment
import sys

def load_gos():
    with open('go.json') as go_file:
        gos = json.loads(go_file.read())

    return { k: set(v) for k, v in gos.items() }

def vertex_fc(v, gos):
    go = gos.get(v['name'], None)
    if go is None:
        print('{} has no gos'.format(v['name']))
        go = set()
    return go

def vertices_fc(v1id, v2id, gos1, gos2):
    num = len(gos1[v1id].intersection(gos2[v2id]))
    denom = len(gos1[v1id].union(gos2[v2id]))
    return -num/denom if denom != 0 else 0


def fc_opt(net1, net2, gos):
    gos1 = [vertex_fc(v, gos) for v in net1.vs]
    gos2 = [vertex_fc(v, gos) for v in net2.vs]

    print('net1 vertices: {}, edges: {}, gos: {}'.format(
            net1.vcount(), net1.vcount(), len([go1 for go1 in gos1 if len(go1) > 0])))

    print('net2 vertices: {}, edges: {}, gos: {}'.format(
            net2.vcount(), net2.vcount(), len([go2 for go2 in gos2 if len(go2) > 0])))

    weights = [ [ vertices_fc(v1.index, v2.index, gos1, gos2) for v2 in net2.vs ] for v1 in net1.vs ]

    result = linear_sum_assignment(weights)

    return result

if __name__ == '__main__':
    assert len(sys.argv) == 4

    net1_path = sys.argv[1]
    net2_path = sys.argv[2]
    out_path = sys.argv[3]

    net1 = igraph.Graph.Read_Ncol('net1.tab', directed=False)
    net2 = igraph.Graph.Read_Ncol('net2.tab', directed=False)
    gos = load_gos()

    result = fc_opt(net1, net2, gos)

    with open(out_path, 'w+') as alignment_file:
        writer = csv.writer(alignment_file, delimiter='\t')
        for i, j in zip(*result):
            namei = net1.vs[i]['name']
            namej = net2.vs[j]['name']
            writer.writerow([namei, namej])
