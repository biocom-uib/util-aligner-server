from os import path
import os
import subprocess

from server.aligners.aligner import Aligner
from server.util import iter_csv


class LGraal(Aligner):
    def __init__(self, alpha=0.5, nthreshold=0.5, iterlimit=1000, timelimit=3600):
        super().__init__()
        # -a [ --alpha ] arg (=0) from 0 (topology) to 1 (sequence)
        self.alpha = alpha
        # -N [ --nthreshold ] arg (=0.5) Node topological similarity threshold
        self.nthreshold = nthreshold
        # -I [ --iterlimit ] arg (=1000) Iteration limit
        self.iterlimit = iterlimit
        # -L [ --timelimit ] arg (=3600) Time limit in seconds
        self.timelimit = timelimit

    @property
    def name(self):
        return 'lgraal'

    @property
    def cmd(self):
        return [
            './l-graal',
            '-a', str(self.alpha),
            '-N', str(self.nthreshold),
            '-I', str(self.iterlimit),
            '-L', str(self.timelimit),
            '-Q', 'net1.gw',
            '-T', 'net2.gw',
            '-q', 'ncount4-net1/net1.ndump2',
            '-t', 'ncount4-net2/net2.ndump2',
            '-B', 'blast-net1-net2.tab',
            '-o', 'alignment-net1-net2.tab'
        ]

    def _fix_leda_for_ncount4(self, in_path, out_path):
        with open(in_path, 'r') as in_f:
            with open(out_path, 'w') as out_f:
                out_f.writelines(line for line in in_f if not line.startswith('#'))

    def _write_net(self, net, net_path):
        net.write_leda(net_path + '.tmp', names='name', weights=None)
        self._fix_leda_for_ncount4(net_path + '.tmp', net_path)
        os.remove(net_path + '.tmp')

    def _run_ncount4(self, run_dir_path, net_path, dest_path):
        ncount4_path = path.join(run_dir_path, 'ncount4')

        os.makedirs(path.dirname(dest_path))

        cmd = [ncount4_path, net_path, dest_path]

        with open(dest_path + '.log', 'w+') as logfile:
            subprocess.check_call(cmd, stdout=logfile, stderr=subprocess.STDOUT)

    def write_files(self, run_dir_path, net1, net2, blast_net1_net2):
        super().write_files(run_dir_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing net1.gw')
        net1_path = path.join(run_dir_path, 'net1.gw')
        self._write_net(net1, net1_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing net2.gw')
        net2_path = path.join(run_dir_path, 'net2.gw')
        self._write_net(net2, net2_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing blast-net1-net2.tab')
        blast_net1_net2_path = path.join(run_dir_path, 'blast-net1-net2.tab')
        blast_net1_net2.write_tricol(blast_net1_net2_path)

        self.logger.info(f'run_{self.name} @ {run_dir_path}: running ncount4 for net1.gw')
        ncount4_net1_path = path.join(run_dir_path, 'ncount4-net1', 'net1')
        self._run_ncount4(run_dir_path, net1_path, ncount4_net1_path)

        self.logger.info(f'run_{self.name} @ {run_dir_path}: running ncount4 for net2.gw')
        ncount4_net2_path = path.join(run_dir_path, 'ncount4-net2', 'net2')
        self._run_ncount4(run_dir_path, net2_path, ncount4_net2_path)

    def import_alignment(self, net1, net2, execution_dir, file_name='alignment-net1-net2.tab'):
        header = (net1.name, net2.name)
        align_path = path.join(execution_dir, file_name)
        return header, [(a.strip(), b.strip()) for a, b in iter_csv(align_path, delimiter='\t')
                                               if a.strip() != '' and b.strip() != '']
