#!/usr/bin/python3

import itertools
import logging
from os import path
import os
import shutil
import subprocess
import tempfile
import time

import util


class Aligner(object):
    def __init__(self):
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)

    @property
    def name(self): return None
    @property
    def env(self): return None
    @property
    def cmd(self): return None

    def write_files(self, run_dir_path):
        pass

    def import_alignment(self, net1, net2, execution_dir, file_name=None):
        pass

    def _gen_run_dir(run_dir_base_path, max_trials):
        for trial in range(max_trials):
            try:
                run_id = uuid.uuid1()
                run_dir_path = path.join(run_dir_base_path, self.name, str(run_id))
                os.makedirs(run_dir_path)
                return run_dir_path
            except OSError:
                pass

        return None

    def _setup_run_dir(self, run_dir_path, template_dir_base_path, *args):
        template_dir_path = path.join(template_dir_base_path, 'template-' + self.name)

        for template_file in os.listdir(template_dir_path):
            template_file_path = path.join(template_dir_path, template_file)

            if path.isdir(template_file_path):
                shutil.copytree(template_file_path, run_dir_path)
            else:
                shutil.copy(template_file_path, run_dir_path)

        self.write_files(run_dir_path, *args)

    def run(self, net1, net2, *args, run_dir_base_path='run', template_dir_base_path='template', max_trials=50):
        os.makedirs(run_dir_base_path, exist_ok=True)

        # run_dir_path =  tempfile.mkdtemp(dir=run_dir_base_path, prefix=self.name + '-'):

        with tempfile.TemporaryDirectory(dir=run_dir_base_path, prefix=self.name + '-') as run_dir_path:
            self.logger.info(f'run_{self.name} @ {run_dir_path}: setting up required files')

            try:
                self._setup_run_dir(run_dir_path, template_dir_base_path, net1, net2, *args)
            except Exception as ex:
                self.logger.exception(f'run_{self.name} @ {run_dir_path}: an exception was raised while setting up the run directory')
                return {'ok': False}

            self.logger.info(f'run_{self.name} @ {run_dir_path}: running')

            result = {'command': self.cmd}

            start_time = time.time()

            try:
                completed_process = subprocess.run(
                    self.cmd,
                    env=self.env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=run_dir_path,
                    check=True)

            except subprocess.CalledProcessError as cpe:
                self.logger.warning(f'run_{self.name} @ {run_dir_path}: process exited with non-zero exit code {cpe.returncode}: {self.cmd}')

                output = cpe.output.decode('utf-8')
                self.logger.info('process output:')
                self.logger.info(output)

                result['ok'] = False
                result['output'] = output
                result['exit_code'] = cpe.returncode

            else:
                end_time = time.time()

                self.logger.info(f'run_{self.name} @ {run_dir_path}: done')

                result['ok'] = True
                result['output'] = completed_process.stdout.decode('utf-8')
                result['exit_code'] = completed_process.returncode

                result['run_time'] = end_time - start_time
                result['alignment_header'], result['alignment'] = self.import_alignment(net1, net2, run_dir_path)

            return result

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
        alignment = [(a,b) for a, b in util.iter_csv(align_path, delimiter=' ', skipinitialspace=False)
                           if a != '' and b != '']

        header = (net1.name, net2.name)
        if net1.igraph.vcount() > net2.igraph.vcount():
            alignment = [(b,a) for a,b in alignment]

        return header, alignment

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
        return header, [(a.strip(), b.strip()) for a, b in util.iter_csv(align_path, delimiter='\t')]

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
        return header, [(a.strip(), b.strip()) for a, b in util.iter_csv(align_path, delimiter='\t')
                                               if a.strip() != '' and b.strip() != '']

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
        return header, [(a, b) for a, b, some_score in util.iter_csv(align_path, delimiter='\t')]

class Spinal(Aligner):
    def __init__(self, alpha=0.7):
        super().__init__()
        self.alpha = alpha

    @property
    def name(self):
        return 'spinal'

    @property
    def cmd(self):
        return [
            './spinal', '-I', '-ns',
            'net1.gml', 'net2.gml', 'blast-net1-net2.csv',
            'alignment-net1-net2.csv',
            str(self.alpha)
        ]

    def write_files(self, run_dir_path, net1, net2, blast_net1_net2):
        super().write_files(run_dir_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing net1.gml')
        net1_path = path.join(run_dir_path, 'net1.gml')
        net1.write_gml(net1_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing net2.gml')
        net2_path = path.join(run_dir_path, 'net2.gml')
        net2.write_gml(net2_path)

        self.logger.debug(f'run_{self.name} @ {run_dir_path}: writing blast-net1-net2.csv')
        blast_path = path.join(run_dir_path, 'blast-net1-net2.csv')
        blast_net1_net2.write_tricol(blast_path, by='index', delimiter=' ')

    # def import_alignment_names(self, execution_dir, file_name='alignment-net1-net2-names.tab'):
    #     align_path = path.join(execution_dir, file_name)
    #     return [(a, b) for a, b in iter_csv(align_path, delimiter='\t')]

    def iter_alignment_ids(self, execution_dir, file_name='alignment-net1-net2.csv'):
        alignment_path = path.join(execution_dir, file_name)

        # ignore the first two lines, they seem to be comments
        csv_rows = util.iter_csv(alignment_path, delimiter=' ')
        csv_rows = itertools.islice(csv_rows, 2, None)

        for p1id, p2id in csv_rows:
            yield int(p1id), int(p2id)

    def iter_alignment(self, net1, net2, execution_dir, file_name='alignment-net1-net2.csv'):
        net1_vs = net1.igraph.vs # sources.read_net_gml(net1.name, path.join(execution_dir, 'net1.gml')).igraph.vs
        net2_vs = net2.igraph.vs # sources.read_net_gml(net2.name, path.join(execution_dir, 'net2.gml')).igraph.vs

        for p1id, p2id in self.iter_alignment_ids(execution_dir, file_name):
            yield net1_vs[p1id]['name'], net2_vs[p2id]['name']

    def import_alignment(self, net1, net2, execution_dir, file_name='alignment-net1-net2.csv'):
        header = (net1.name, net2.name)
        return header, list(self.iter_alignment(net1, net2, execution_dir, file_name))

def import_fcopt_alignment(net1, net2, execution_dir):
    align_path = path.join(execution_dir, 'alignment-net1-net2.tab')
    header = (net1.name, net2.name)
    return header, [(a, b) for a, b in util.iter_csv(align_path, delimiter='\t')]

