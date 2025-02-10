### MODULE TO HANDLE DATABRICKSCLI INTEGRATION ###

import pandas as pd
from time import sleep
import os
import json
import datetime


class JobsCLI:

    def __init__(self, job_id, path='C:\Temp'):
        self.job_id = job_id
        self.path = path

    def print_time(self, complete_path):
        command = r'echo %date%-%time% > {}'.format(complete_path)
        stream = os.popen(command)

    def job_run(self, filename="run_id.json", notebook_params=None):
        complete_path = os.path.join(self.path, filename)
        if notebook_params:
            command = """databricks jobs run-now --job-id {} -- notebook-params '{}'> {}""".format(
                self.job_id, str(notebook_params).replace("'", '"'), complete_path)
        else:
            command = "databricks jobs run-now --job-id {} > {}".format(
                self.job_id, complete_path)
        stream = os.popen(command)
        sleep(5)
        with open(complete_path) as f:
            dict_metadata = json.load(f)
        self.run_id = dict_metadata['run_id']

    def get_job_metadata(self, filename="job_metadata.json"):
        complete_path = os.path.join(self.path, filename)
        command = 'databricks jobs get --job-id {} > {}'.format(
            self.job_id, complete_path)
        stream = os.popen(command)
        sleep(5)
        with open(complete_path) as f:
            dict_metadata = json.load(f)
        return dict_metadata

    def cancel_job_run(self, outside_run_id=None):
        if outside_run_id:
            command = 'databricks runs cancel --run-id {}'.format(
                outside_run_id)
        else:
            command = 'databricks runs cancel --run-id {}'.format(self.run_id)
        stream = os.popen(command)

    def get_run_metadata(self, filename="run_metadata.json", outside_run_id=None):
        complete_path = os.path.join(self.path, filename)
        if outside_run_id:
            command = 'databricks runs get --run-id {} > {}'.format(
                outside_run_id, complete_path)
        else:
            command = 'databricks runs get --run-id {} > {}'.format(
                self.run_id, complete_path)
        stream = os.popen(command)
        sleep(5)
        with open(complete_path) as f:
            dict_metadata = json.load(f)
        return dict_metadata

    def get_run_output(self, filename="run_output_metadata.json", outside_run_id=None):
        complete_path = os.path.join(self.path, filename)
        if outside_run_id:
            command = 'databricks runs get-output --run-id {} > {}'.format(outside_run_id,
                                                                           complete_path)
        else:
            command = 'databricks runs get-output --run-id {} > {}'.format(self.run_id,
                                                                           complete_path)
        stream = os.popen(command)
        sleep(5)
        with open(complete_path) as f:
            dict_metadata = json.load(f)
        return dict_metadata


class DbfsCLI:

    def copy(self, path_ori, path_dest, overwrite=True):
        if overwrite:
            command = 'dbfs cp "{}" "{}" --overwrite'.format(
                path_ori, path_dest)
        else:
            command = 'dbfs cp "{}" "{}"'.format(path_ori, path_dest)
        print(command)
        stream = os.popen(command)
        output = [x.strip() for x in stream.readlines()]

    def remove(self, path):
        command = 'dbfs rm "{}"'.format(path)
        stream = os.popen(command)
        output = [x.strip() for x in stream.readlines()]
        return output[1]

    def move(self, path_ori, path_dest):
        command = 'dbfs mv "{}" "{}"'.format(path_ori, path_dest)
        stream = os.popen(command)

    def list_files(self, path, absolute='--absolute', l='-l'):
        command = 'dbfs ls "{}" {} {}'.format(path, absolute, l)
        stream = os.popen(command)
        if l == '-l':
            output = [x.strip().split(' ') for x in stream.readlines()]
            for x in output:
                while '' in x:
                    x.remove('')
            dict_infos = {x[2]: {'tipo': x[0], 'tamanho': x[1]}
                          for x in output}
            return dict_infos
        else:
            output = [x.strip() for x in stream.readlines()]
            return output

    def copy_and_run(self, local_path, dbfs_path, job_id, int_seconds_wait=10):
        """
        DOCSTRING: COPY CSV FILES TO DBFS IN DATABRICKS WORKSPACE THAN RUN THE JOB TO UPLOAD THE DATA
        INPUTS: LOCAL PATH, DBFS PATH AND THE JOB ID IN DATABRICKS
        OUTPUTS: -
        """
        self.copy(local_path, dbfs_path)
        tables = self.list_files('dbfs:/FileStore/tables')
        while dbfs_path not in tables.keys():
            self.copy(local_path, dbfs_path)
            tables = self.list_files('dbfs:/FileStore/tables')
        sleep(int_seconds_wait)
        db_cli = JobsCLI(job_id)
        db_cli.job_run()
        sleep(int_seconds_wait)
        try:
            run_state = db_cli.get_run_metadata()
        except json.JSONDecodeError:
            print('Unable to fetch metadata from run: {}'.format(db_cli.run_id))
        sleep(int_seconds_wait)
        while run_state['state']['life_cycle_state'] != 'TERMINATED':
            try:
                run_state = db_cli.get_run_metadata()
            except json.JSONDecodeError:
                print('Unable to fetch metadata from run: {}'.format(db_cli.run_id))
            sleep(int_seconds_wait)
        # status of accomplishment
        if run_state['state']['result_state'] != 'SUCCESS':
            return 'Error running job'
        else:
            return 'Success running job'
