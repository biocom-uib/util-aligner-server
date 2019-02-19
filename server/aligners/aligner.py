import logging
from os import path
import os
import pandas as pd
import shutil
import subprocess
import tempfile
import time


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
                header, alignment = self.import_alignment(net1, net2, run_dir_path)

                columns = [f'net1_{header[0]}', f'net2_{header[1]}']

                alignment_df = pd.DataFrame(alignment, columns=columns)
                alignment_df.set_index(columns[0], inplace=True)

                result['alignment'] = alignment_df

            return result
