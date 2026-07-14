import json
import dataclasses
import secrets
from typing import Optional
from executor.sandbox import Sandbox, JudgeError


class SubmissionRunner:

    def __init__(
        self,
        job_id: str,
        time_limit: int,  # sec.
        mem_limit: int,  # KB
        testdata_input_path: str,
        testdata_output_path: str,
        special_judge: bool = False,
        lang: Optional[str] = None,
        case_no: Optional[str] = None,
    ):
        # config file
        with open('.config/submission.json') as f:
            config = json.load(f)
        self.lang = lang
        self.special_judge = special_judge
        self.case_no = case_no
        # required
        self.job_id = job_id
        self.time_limit = time_limit
        self.mem_limit = mem_limit
        self.testdata_input_path = testdata_input_path  # absoulte path str
        self.testdata_output_path = testdata_output_path  # absoulte path str
        # working_dir
        self.working_dir = config['working_dir']
        # for language specified settings
        self.lang_id = config['lang_id']
        self.image = config['image']

    def container_name(self, phase: str) -> str:
        # random suffix: a reclaimed job can rerun while a zombie container
        # from a previous attempt is still alive; a fixed name would collide
        return f'{self.job_id}-{phase}-{secrets.token_hex(3)}'

    def compile(self):
        try:
            # compile must be done in 20 seconds
            result = Sandbox(
                time_limit=20000,  # 20s
                mem_limit=1048576,  # 1GB
                image=self.image[self.lang],
                src_dir=f'{self.working_dir}/{self.job_id}/src',
                lang_id=self.lang_id[self.lang],
                compile_need=True,
                name=self.container_name('compile'),
            ).run()
        except JudgeError:
            return {'Status': 'JE'}
        if result.Status == 'Exited Normally':
            result.Status = 'AC'
        else:
            result.Status = 'CE'
        return dataclasses.asdict(result)

    def run(self):
        try:
            result = Sandbox(
                time_limit=self.time_limit,
                mem_limit=self.mem_limit,
                image=self.image[self.lang],
                src_dir=f'{self.working_dir}/{self.job_id}/src',
                lang_id=self.lang_id[self.lang],
                compile_need=False,
                stdin_path=self.testdata_input_path,
                name=self.container_name(self.case_no or 'run'),
            ).run()
        except JudgeError:
            return {'Status': 'JE'}
        with open(self.testdata_output_path, 'r') as f:
            ans_output = f.read()
        status = {'TLE', 'MLE', 'RE', 'OLE'}
        if result.Status not in status:
            result.Status = 'WA'
            res_outs = self.strip(result.Stdout)
            ans_outputs = self.strip(ans_output)
            if res_outs == ans_outputs:
                result.Status = 'AC'
        return dataclasses.asdict(result)

    @classmethod
    def strip(cls, s: str) -> list:
        # strip trailing space for each line
        ss = [s.rstrip() for s in s.splitlines()]
        # strip redundant new line
        while len(ss) and ss[-1] == '':
            del ss[-1]
        return ss
