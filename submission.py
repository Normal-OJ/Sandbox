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
        self.lang_id = config['lang_id']
        self.image = config['image']

    def compile(self):
        # compile must be done in 10 seconds
        s = Sandbox(
            time_limit=10000,  # 10s
            mem_limit=1048576,  # 1GB
            image=self.image[self.lang],
            src_dir=f'{self.working_dir}/{self.submission_id}/src',
            lang_id=self.lang_id[self.lang],
            compile_need=1)
        result = s.run()
        result[
            'Status'] = 'AC' if result['Status'] == 'Exited Normally' else 'CE'
        return result

    def run(self):
        s = Sandbox(time_limit=self.time_limit,
                    mem_limit=self.mem_limit,
                    image=self.image[self.lang],
                    src_dir=f'{self.working_dir}/{self.submission_id}/src',
                    lang_id=self.lang_id[self.lang],
                    compile_need=0,
                    stdin_path=self.testdata_input_path)
        result = s.run()
        # Status Process
        with open(self.testdata_output_path, 'r') as f:
            ans_output = f.read()
        status = ['TLE', 'MLE', 'RE', 'OE', 'SE']
        if not result['Status'] in status:
            result['Status'] = 'AC' if result['Stdout'] == ans_output else 'WA'
        return result
