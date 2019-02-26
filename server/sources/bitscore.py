import numpy as np
import pandas as pd

from server.util import iter_csv, write_csv


class BitscoreMatrix(object):
    def __init__(self):
        pass

    def to_dataframe(self, by='name'):
        index_columns = [f'{by}1', f'{by}2']

        return pd.DataFrame.from_records(
                self.iter_tricol(by=by),
                columns = index_columns + ['bitscore'],
                index = index_columns) \
            .astype({'bitscore': float})

    def write_tricol(self, file_path, by='name', **kwargs):
        if 'delimiter' not in kwargs:
            kwargs['delimiter'] = '\t'

        write_csv(file_path, self.iter_tricol(by=by), **kwargs)


class TricolBitscoreMatrix(BitscoreMatrix):
    def __init__(self, tricol, net1=None, net2=None, by='name'):
        self.tricol = np.array(tricol)
        self.net1 = net1
        self.net2 = net2
        self.by = by

    def swapping_net1_net2(self):
        return TricolBitscoreMatrix(self.tricol[:,[1,0,2]], net1=self.net2, net2=self.net1, by=self.by)

    def to_dataframe(self, by='name'):
        index_columns = [f'{by}1', f'{by}2']

        return pd.DataFrame.from_records(
                self.to_numpy(by=by),
                columns = index_columns + ['bitscore'],
                index = index_columns) \
            .astype({'bitscore': float})

    def to_numpy(self, by='name'):
        if by == self.by:
            return self.tricol
        else:
            return np.array(list(self.iter_tricol(by=by)))

    def iter_tricol(self, by='name'):
        self_by = self.by

        if by == self_by:
            yield from self.tricol

        elif by == 'object':
            if self.net1 is None or self.net2 is None:
                raise ValueError(f"must specify net1 and net2 in order to use different 'by' values (current: {self.by}, given: {by})")

            net1_vs = self.net1.igraph.vs
            net2_vs = self.net2.igraph.vs

            if self_by == 'index':
                for p1_id, p2_id, score in self.tricol:
                    yield net1_vs[p1_id], net2_vs[p2_id], score
            else:
                for p1_by, p2_by, score in self.tricol:
                    p1s = net1_vs.select(**{self_by:p1_by})
                    p2s = net2_vs.select(**{self_by:p2_by})
                    if len(p1s) > 0 and len(p2s) > 0:
                        yield p1s[0], p2s[0], score

        elif by == 'index':
            for p1, p2, score in self.iter_tricol(by='object'):
                yield p1.index, p2.index, score

        else:
            for p1, p2, score in self.iter_tricol(by='object'):
                yield p1[by], p2[by], score


def read_tricol_bitscores(file_path, net1=None, net2=None, by='name', row_filter=None, **kwargs):
    if 'delimiter' not in kwargs:
        kwargs['delimiter'] = '\t'

    if row_filter is not None:
        rows = [row for row in iter_csv(file_path, **kwargs) if row_filter(row)]
    else:
        rows = [row for row in iter_csv(file_path, **kwargs)]

    return TricolBitscoreMatrix(rows, net1=net1, net2=net2, by=by)

def identity_bitscores(net, by='name'):
    return TricolBitscoreMatrix([(v, v, 1) for v in net.iter_vertices(by=by)], by=by)

