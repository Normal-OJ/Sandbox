import os
import shutil
from typing import BinaryIO
from datetime import datetime
from zipfile import ZipFile
from pathlib import Path
from . import config
from .meta import Meta
from .utils import logger


def extract(
    root_dir: Path,
    submission_id: str,
    meta: Meta,
    source: BinaryIO,
    testdata: Path,
):
    submission_dir = root_dir / submission_id
    submission_dir.mkdir()
    (submission_dir / 'meta.json').write_text(meta.json())
    logger().debug(f'{submission_id}\'s meta: {meta}')
    for i, task in enumerate(meta.tasks):
        if task.caseCount == 0:
            logger().warning(f'empty task. [id={submission_id}/{i:02d}]')
    # extract source code
    code_dir = submission_dir / 'src'
    code_dir.mkdir()
    with ZipFile(source) as zf:
        zf.extractall(code_dir)
    # check
    files = [*code_dir.iterdir()]
    if len(files) == 0:
        raise ValueError('no file in \'src\' directory')
    language_id = int(meta.language)
    language_type = ['.c', '.cpp', '.py'][language_id]
    for _file in files:
        if _file.stem != 'main':
            raise ValueError('none main')
        if _file.suffix != language_type:
            raise ValueError('data type is not match')
    # copy testdata
    testcase_dir = submission_dir / 'testcase'
    shutil.copytree(testdata, testcase_dir)
    # move chaos files to src directory
    chaos_dir = testcase_dir / 'chaos'
    if chaos_dir.exists():
        if chaos_dir.is_file():
            raise ValueError('\'chaos\' can not be a file')
        for chaos_file in chaos_dir.iterdir():
            shutil.move(str(chaos_file), str(code_dir))
        os.rmdir(chaos_dir)


def clean_data(submission_id):
    submission_dir = config.SUBMISSION_DIR / submission_id
    shutil.rmtree(submission_dir)


def backup_data(submission_id):
    submission_dir = config.SUBMISSION_DIR / submission_id
    dest = config.SUBMISSION_BACKUP_DIR / f'{submission_id}_{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}'
    shutil.move(submission_dir, dest)
