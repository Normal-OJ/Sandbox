import os
import random
import json
import pathlib
from tests.problem_parser import ProblemParser


class SubmissionGenerator:
    def __init__(
        self,
        submission_path='submissions',
        problem_parser=None,
        up=10**8,
        down=0,
    ):
        # prepare submission folder
        self.submission_path = pathlib.Path(submission_path)
        if not self.submission_path.exists():
            os.mkdir(self.submission_path)
        elif self.submission_path.is_file():
            raise FileExistsError(
                f'The {self.submission_path} is a file! not a directory.')

        # parse problem data
        if problem_parser is None:
            problem_parser = ProblemParser()
        self.problem = problem_parser.parse()

        # Dict[submission_id, prob_name]
        self.submission_ids = {}

        # bound for submission id
        self.up = up
        self.down = down

    def gen_submission_id(self):
        return str(random.randint(self.down, self.up))

    def get_submission_path(self, submission_id):
        return str((self.submission_path / str(submission_id)).absolute())

    def gen_submission(self, prob_name=None, submission_id=None):
        # process args
        if prob_name is None:
            prob_name = random.choice(self.problem.keys())
        if submission_id is None:
            submission_id = self.gen_submission_id()
            while submission_id in self.submission_ids:
                submission_id = self.gen_submission_id()
            self.submission_ids[submission_id] = prob_name

        prob_data = self.problem[prob_name]
        prob_base_dir = self.submission_path / submission_id
        prob_base_dir.mkdir()

        # write source
        os.mkdir(prob_base_dir / 'src')
        for src in prob_data['source']:
            with open(f'{prob_base_dir}/src/{src}', 'w') as f:
                f.write(prob_data['source'][src])

        # write testcase
        testcase_dir = prob_base_dir / 'testcase'
        testcase_dir.mkdir()
        for i, task in enumerate(prob_data['meta']['tasks']):
            for j in range(task['caseCount']):
                with open(f'{testcase_dir}/{i:02d}{j:02d}.in', 'w') as f:
                    f.write(prob_data['testcase'][i][j]['in'])
                with open(f'{testcase_dir}/{i:02d}{j:02d}.out', 'w') as f:
                    f.write(prob_data['testcase'][i][j]['out'])

        # write meta
        with open(f'{prob_base_dir}/meta.json', 'w') as f:
            json.dump(prob_data['meta'], f)

    def gen_all(self):
        # generate submission id
        submission_ids = set()
        while len(submission_ids) < len(self.problem):
            submission_id = self.gen_submission_id()
            while submission_id in self.submission_ids:
                submission_id = self.gen_submission_id()
            submission_ids.add(submission_id)
        submission_ids = list(submission_ids)

        for prob, submission_id in zip(self.problem, submission_ids):
            self.submission_ids[submission_id] = prob
            self.gen_submission(prob, submission_id)

        return submission_ids

    def clear(self):
        import shutil
        shutil.rmtree(self.submission_path)