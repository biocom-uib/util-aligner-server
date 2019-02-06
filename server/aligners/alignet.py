from os import path

from server.aligners.aligner import Aligner
from server.util import iter_csv


class Alignet(Aligner):
    def __init__(self, threads=1):
        super().__init__()
        self.threads = threads

    @property
    def name(self):
        return 'alignet'

    @property
    def env(self):
        #r_lib_dir = path.join(path.expanduser('~'), 'r-library')
        #os.makedirs(r_lib_dir)
        #{'R_LIBS': r_lib_dir}
        return {'ALIGNET_NUM_THREADS': str(self.threads)}

    @property
    def cmd(self):
        return ['Rscript', '--vanilla', 'alignet.R']

    def write_files(self, run_dir_path, net1, net2, blast_net1, blast_net2, blast_net1_net2):
        super().write_files(run_dir_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing net1.tab')
        net1_path = path.join(run_dir_path, 'net1.tab')
        net1.write_tsv_edgelist(net1_path, header=['INTERACTOR_A', 'INTERACTOR_B'])

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing net2.tab')
        net2_path = path.join(run_dir_path, 'net2.tab')
        net2.write_tsv_edgelist(net2_path, header=['INTERACTOR_A', 'INTERACTOR_B'])

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing blast-net1.tab')
        blast_net1_path = path.join(run_dir_path, 'blast-net1.tab')
        blast_net1.write_tricol(blast_net1_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing blast-net2.tab')
        blast_net2_path = path.join(run_dir_path, 'blast-net2.tab')
        blast_net2.write_tricol(blast_net2_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing blast-net1-net2.tab')
        blast_net1_net2_path = path.join(run_dir_path, 'blast-net1-net2.tab')
        blast_net1_net2.write_tricol(blast_net1_net2_path)

    def import_alignment(self, net1, net2, execution_dir, file_name='alignment-net1-net2.tab'):
        header = (net1.name, net2.name)
        align_path = path.join(execution_dir, file_name)
        return header, [(a.strip(), b.strip()) for a, b in iter_csv(align_path, delimiter='\t')]
