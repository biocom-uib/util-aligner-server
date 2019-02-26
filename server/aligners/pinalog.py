from os import path

from server.aligners.aligner import Aligner
from server.util import iter_csv


class Pinalog(Aligner):
    def __init__(self):
        super().__init__()

    @property
    def name(self):
        return 'pinalog'

    @property
    def cmd(self):
        return ['./pinalog1.0', 'net1.tab', 'net2.tab', 'blast-net1-net2.tab']

    def write_files(self, run_dir_path, net1, net2, blast_net1_net2):
        super().write_files(run_dir_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing net1.tab')
        net1_path = path.join(run_dir_path, 'net1.tab')
        net1.write_tsv_edgelist(net1_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing net2.tab')
        net2_path = path.join(run_dir_path, 'net2.tab')
        net2.write_tsv_edgelist(net2_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing blast-net1-net2.tab')
        blast_path = path.join(run_dir_path, 'blast-net1-net2.tab')
        blast_net1_net2.write_tricol(blast_path)

    def import_alignment(self, net1, net2, execution_dir, file_name='net1_net2.pinalog.nodes_algn.txt'):
        header = (net1.name, net2.name)
        align_path = path.join(execution_dir, file_name)
        return header, [(a, b) for a, b, some_score in iter_csv(align_path, delimiter='\t')]
