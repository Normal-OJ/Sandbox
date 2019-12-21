import json

import docker

from sandbox import Sandbox


class SubmissionRunner():
    def __init__(self,
                 submission_id,
                 time_limit,
                 mem_limit,
                 testdata_input_path,
                 testdata_output_path,
                 special_judge=False,
                 lang=None):
        # config file
        with open('.config/submission.json') as f:
            config = json.load(f)
        # optional
        self.lang = lang  # str
        self.special_judge = special_judge  # bool
        # required
        self.submission_id = submission_id  # str
        self.time_limit = time_limit  # int s
        self.mem_limit = mem_limit  # int kb
        self.testdata_input_path = testdata_input_path  # absoulte path str
        self.testdata_output_path = testdata_output_path  # absoulte path str
        # working_dir
        self.working_dir = config['working_dir']
        # for language specified settings
        self.compile_argument = config['compile_argument']
        self.execute_argument = config['execute_argument']
        self.image = config['image']

    def compile(self):
        compile_command = self.compile_argument[self.lang]
        # compile must be done in 10 seconds
        s = Sandbox(
            time_limit=10,  # 10s
            mem_limit=1048576,  # 1GB
            image=self.image[self.lang],
            src_dir=f'{self.working_dir}/{self.submission_id}/src',
            command=compile_command,
            compile=True)
        result = s.run()
        # Status Process
        if result['DockerExitCode']:
            result['Status'] = 'CE'
        else:
            result['Status'] = 'Pass'
        return result

    def run(self):
        execute_command = f'{self.execute_argument[self.lang]}'
        s = Sandbox(time_limit=self.time_limit,
                    mem_limit=self.mem_limit,
                    image=self.image[self.lang],
                    src_dir=f'{self.working_dir}/{self.submission_id}/src',
                    command=execute_command,
                    compile=False,
                    stdin_path=self.testdata_input_path)
        result = s.run()
        # Status Process
        with open(self.testdata_output_path, 'r') as f:
            ans_output = f.read()
        status = ['TLE', 'MLE', 'RE', 'OE']
        if not result['Status'] in status:
            result['Status'] = 'AC' if result['Stdout'] == ans_output else 'WA'
        return result
