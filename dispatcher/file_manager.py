import os
import shutil
from datetime import datetime
from zipfile import ZipFile
from pathlib import Path
from . import config
from .meta import Meta
from .utils import logger


def extract(
    root_dir: Path,
    job_id: str,
    meta: Meta,
    source,
    testdata: Path,
):
    job_dir = root_dir / job_id
    job_dir.mkdir()
    (job_dir / 'meta.json').write_text(meta.json())
    logger().debug(f'{job_id}\'s meta: {meta}')
    for i, task in enumerate(meta.tasks):
        if task.caseCount == 0:
            logger().warning(f'empty task. [id={job_id}/{i:02d}]')
    # extract source code
    code_dir = job_dir / 'src'
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
    testcase_dir = job_dir / 'testcase'
    shutil.copytree(testdata, testcase_dir)
    # move chaos files to src directory
    chaos_dir = testcase_dir / 'chaos'
    if chaos_dir.exists():
        if chaos_dir.is_file():
            raise ValueError('\'chaos\' can not be a file')
        for chaos_file in chaos_dir.iterdir():
            shutil.move(str(chaos_file), str(code_dir))
        os.rmdir(chaos_dir)


def clean_data(job_id):
    job_dir = config.SUBMISSION_DIR / job_id
    shutil.rmtree(job_dir)


def backup_data(job_id):
    job_dir = config.SUBMISSION_DIR / job_id
    dest = config.SUBMISSION_BACKUP_DIR / f'{job_id}_{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}'
    shutil.move(job_dir, dest)
