import os
import random
import pathlib
import logging
import secrets
import io
from zipfile import ZipFile
from typing import Dict, Optional
from dispatcher import file_manager
from dispatcher.meta import Meta


class SubmissionGenerator:

    def __init__(
        self,
        submission_dir: str = 'submissions',
        problem_dir: str = 'tests/problem',
    ):
        # prepare submission directory
        self.submission_dir = pathlib.Path(submission_dir)
        if not self.submission_dir.exists():
            os.mkdir(self.submission_dir)
        elif self.submission_dir.is_file():
            raise NotADirectoryError(self.submission_dir)
        self.problem_dir = pathlib.Path(problem_dir)
        # Dict[submission_id, prob_name]
        self.submission_ids: Dict[str, str] = {}

    def gen_submission_id(self) -> str:
        '''
        generate a submission id
        '''
        current_ids = {*map(str, self.submission_dir.iterdir())}
        _id = secrets.token_bytes()[:12]
        while _id in current_ids:
            _id = secrets.token_bytes()[:12]
        return _id.hex()

    def get_submission_path(self, submission_id: str) -> str:
        '''
        return absolute path of specific submission
        '''
        return str((self.submission_dir / str(submission_id)).absolute())

    def gen_submission(
        self,
        prob_name: Optional[str] = None,
        submission_id: Optional[str] = None,
    ) -> str:
        if prob_name is None:
            prob_name = random.choice(
                [p.name for p in self.problem_dir.iterdir()])
        if submission_id is None:
            submission_id = self.gen_submission_id()
            self.submission_ids[submission_id] = prob_name
        prob_dir = self.problem_dir / prob_name

        # copy source
        src = io.BytesIO()
        with ZipFile(src, 'x') as zf:
            for s in (prob_dir / 'src').iterdir():
                zf.write(s, s.name)
        src.seek(0)

        file_manager.extract(
            root_dir=self.submission_dir,
            submission_id=submission_id,
            meta=Meta.parse_file(prob_dir / 'meta.json'),
            source=src,
            testdata=prob_dir / 'testcase',
        )

        return submission_id

    def gen_all(self):
        # generate submissions
        for prob in self.problem_dir.iterdir():
            self.gen_submission(prob.name)
        return self.submission_ids

    def clear(self):
        logging.info(f'clear {self.submission_dir}')
        import shutil
        shutil.rmtree(self.submission_dir)
