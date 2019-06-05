import numpy as np
import pandas as pd


class BitscoreMatrix(object):
    def __init__(self, net1, net2, by):
        self._tricol_df = None
        self.net1 = net1
        self.net2 = net2
        self.by = by


    @property
    def dataframe(self):
        if self._tricol_df is None:
            self._tricol_df = self.to_dataframe(by = self.by)
        return self._tricol_df


    def to_dataframe(self, by='name'):
        if self._tricol_df is not None and by == self.by:
            return self._tricol_df

        return pd.DataFrame(
                self.iter_tricol(by=by),
                columns = [f'{by}1', f'{by}2', 'bitscore']) \
            .astype({'bitscore': float})


    def iter_tricol(self, by='name'):
        raise NotImplementedError()


    def write_tricol(self, file_path, by='name', **kwargs):
        if 'sep' not in kwargs:
            kwargs['sep'] = '\t'

        if 'header' not in kwargs:
            kwargs['header'] = False

        self.to_dataframe(by=by).to_csv(file_path, index=False, **kwargs)


class TricolBitscoreMatrix(BitscoreMatrix):
    def __init__(self, tricol, net1, net2, by='name'):
        super().__init__(net1, net2, by)

        col_names = [f'{by}1', f'{by}2', 'bitscore']

        if isinstance(tricol, pd.DataFrame):
            self._tricol_df = tricol.copy(deep=False)
            self._tricol_df.columns = col_names
        else:
            self._tricol_df = pd.DataFrame(tricol, columns=col_names)

        self._tricol_df = self._tricol_df.astype({'bitscore': float})


    def swapping_net1_net2(self):
        return TricolBitscoreMatrix(self.dataframe.iloc[:,[1,0,2]], net1=self.net2, net2=self.net1, by=self.by)


    def to_numpy(self, by='name'):
        return np.array(list(self.iter_tricol(by=by)))


    def iter_tricol(self, by='name'):
        self_by = self.by

        if by == self_by:
            yield from self.dataframe.itertuples(index=False, name=None)

        elif by == 'object':
            if self.net1 is None or self.net2 is None:
                raise ValueError(f"must specify net1 and net2 in order to use different 'by' values (current: {self.by}, given: {by})")

            net1_vs = self.net1.igraph.vs
            net2_vs = self.net2.igraph.vs

            if self_by == 'index':
                for p1_id, p2_id, score in self.dataframe:
                    yield net1_vs[p1_id], net2_vs[p2_id], score
            else:
                for p1_by, p2_by, score in self.dataframe:
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


def read_tricol_bitscores(file_path, net1=None, net2=None, by='name', **kwargs):
    if 'sep' not in kwargs:
        kwargs['sep'] = '\t'

    df = pd.read_csv(file_path, **kwargs)

    return TricolBitscoreMatrix(df, net1=net1, net2=net2, by=by)


def identity_bitscores(net, by='name'):
    df = pd.DataFrame([(v, v, 1) for v in net.iter_vertices(by=by)])

    return TricolBitscoreMatrix(df, net1=net, net2=net, by=by)

