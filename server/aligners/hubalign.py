from os import path

from server.aligners.aligner import Aligner
from server.util import iter_csv


class Hubalign(Aligner):
    def __init__(self, lambda_=0.1, alpha=0.7, d=10):
        super().__init__()
        # The parameter λ that controls the importance of the edge weight
        # relative to the node weight. Usually λ=0.1 yields a biologically more
        # meaningful alignment. Default value is equal to 0.1.
        self.lambda_ = lambda_

        # The parameter α that controls the contribution of sequence
        # information relative to topological similarity. Default value for
        # this parameter is equal to 0.7. α=1 implies that only topological
        # information is used.
        self.alpha = alpha

        # the parameter d that controls the number of nodes removed from
        # network in the process of making the skeleton.
        self.d = d

    @property
    def name(self):
        return 'hubalign'

    @property
    def cmd(self):
        ret = ['./HubAlign',
            'net1.tab', 'net2.tab',
            '-l', str(self.lambda_),
            '-a', str(self.alpha),
            '-d', str(self.d)
        ]

        if self.alpha < 1:
            ret += ['-b', 'blast-net1-net2.tab']

        return ret

    def write_files(self, run_dir_path, net1, net2, blast_net1_net2):
        super().write_files(run_dir_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing net1.tab')
        net1_path = path.join(run_dir_path, 'net1.tab')
        net1.write_tsv_edgelist(net1_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing net2.tab')
        net2_path = path.join(run_dir_path, 'net2.tab')
        net2.write_tsv_edgelist(net2_path)

        if blast_net1_net2 is not None:
            self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing blast-net1-net2.tab')
            blast_path = path.join(run_dir_path, 'blast-net1-net2.tab')
            blast_net1_net2.write_tricol(blast_path)
        elif self.alpha < 1:
            raise ValueError('must provide a BLAST matrix whenever alpha < 1')

    def import_alignment(self, net1, net2, execution_dir, file_name='net1.tab-net2.tab.alignment'):
        align_path = path.join(execution_dir, file_name)
        alignment = [(a,b) for a, b in iter_csv(align_path, delimiter=' ', skipinitialspace=False)
                           if a != '' and b != '']

        header = (net1.name, net2.name)
        if net1.igraph.vcount() > net2.igraph.vcount():
            alignment = [(b,a) for a,b in alignment]

        return header, alignment
