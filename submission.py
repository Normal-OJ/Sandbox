import json

import docker

from sandbox import Sandbox


class SubmissionRunner():
    """docstring for submission_runner"""
    def __init__(self,
                 submission_id,
                 time_limit,
                 mem_limit,
                 special_judge=False,
                 testdata_input=None,
                 testdata_output=None,
                 lang=None):
        # config file
        with open('.config/submission.json') as f:
            config = json.load(f)
        # optional
        self.lang = lang  # str
        self.special_judge = special_judge  # bool
        # required
        self.submission_id = submission_id  # str
        self.time_limit = time_limit  # int ms
        self.mem_limit = mem_limit  # int kb
        self.testdata_input = testdata_input # str
        self.testdata_output = testdata_output # str
        # working_dir
        self.working_dir = config['working_dir']
        # for language specified settings
        self.compile_argument = config['compile_argument']
        self.execute_argument = config['execute_argument']
        self.image = config['image']

    def compile(self):
        compile_command = self.compile_argument[self.lang]
        # compile must be done in 10 seconds
        s = Sandbox(time_limit=10000,
                    mem_limit=self.mem_limit,
                    image=self.image[self.lang],
                    src_dir=f'{self.working_dir}/{self.submission_id}/src',
                    command=compile_command,
                    volume_readonly=False)
        result = s.run()
        return result

    def run(self):
        execute_command = self.execute_argument[self.lang]
        s = Sandbox(time_limit=self.time_limit,
                    mem_limit=self.mem_limit,
                    image=self.image[self.lang],
                    src_dir=f'{self.working_dir}/{self.submission_id}/src',
                    command=execute_command)
        result = s.run()
        return result
