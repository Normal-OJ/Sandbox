import os
import json
import zipfile
import glob
import pathlib
import logging
import shutil
import requests

from logging.config import dictConfig
from flask import Flask, request, jsonify
from os import walk
from dispatcher.dispatcher import Dispatcher

logging.basicConfig(filename='sandbox.log', level=logging.DEBUG)

app = Flask(__name__)
logger = app.logger

# setup constant

# data storage
SUBMISSION_DIR = pathlib.Path(os.environ.get('SUBMISSION_DIR', 'submissions'))
TMP_DIR = pathlib.Path(os.environ.get('TMP_DIR', '/tmp/submissions'))

# create directory
SUBMISSION_DIR.mkdir(exist_ok=True)
TMP_DIR.mkdir(exist_ok=True)

# setup dispatcher
DISPATCHER_CONFIG = os.environ.get('DISPATCHER_CONFIG',
                                   '.config/dispatcher.json.example')
DISPATCHER = Dispatcher(DISPATCHER_CONFIG)
DISPATCHER.start()

# backend config
BACKEND_PORT = os.environ.get('BACKEND_PORT', 8080)
BACKEND_API = os.environ.get('BACKEND_API', f'http://web:{BACKEND_PORT}')

tokens = {}
cookies = {}


@app.route('/submit/<submission_id>', methods=['POST'])
def submit(submission_id):
    # get cookies
    cookies[submission_id] = request.cookies

    languages = ['c', 'cpp', 'py']
    zip_dir = TMP_DIR / submission_id
    zip_dir.mkdir()

    checker = request.values['checker']
    try:
        # 0:C, 1:C++, 2:python3
        language_id = int(request.values['languageId'])
        language_type = languages[language_id]
    except ValueError:
        return 'invalid language id', 400
    except IndexError:
        return "language id wrong-400", 400

    token = request.values['token']
    tokens[submission_id] = token
    while type(submission_id) != str:
        return "submission id wrong-400", 400

    submission_dir = SUBMISSION_DIR / submission_id
    submission_dir.mkdir()

    code = request.files['code']  # get file
    code_path = zip_dir / 'code.zip'
    code.save(str(code_path))  # save file
    code_dir = submission_dir / 'src'
    code_dir.mkdir()
    archive = zipfile.ZipFile(code_path, 'r')
    # file_name=archive.filename.split('.')[0]#filename
    archive.extractall(str(code_dir))
    archive.close()

    # extract testcase zip
    testcase = request.files['testcase']
    testcase_zip = zip_dir / 'testcase.zip'
    testcase.save(str(testcase_zip))
    testcase_dir = submission_dir / 'testcase'
    testcase_dir.mkdir()
    with zipfile.ZipFile(testcase_zip, 'r') as f:
        f.extractall(str(testcase_dir))

    # archize_src = zipfile.ZipFile(TMP_DIR+submission_id+'/src.zip', 'r')
    # archize_src.extractall(TMP_DIR+submission_id)
    # archize_src.close()

    # archize_testcase = zipfile.ZipFile(
    #     TMP_DIR+submission_id+'/testcase.zip', 'r')
    # archize_testcase.extractall(TMP_DIR+submission_id)
    # archize_testcase.close()

    # len(['1','2','3'])
    if len(os.listdir(submission_dir / 'src')) == 0:
        return "under src does not have any file-400", 400
    else:
        for files in (submission_dir / 'src').iterdir():
            file_name = files.name.split('.')[0]  # get file name
            if file_name != 'main':
                return 'none main-400', 400

            which_file = files.name.split('.')[1]  # 'py'
            if which_file != language_type:
                return 'data type is not match-400', 400

            # target_path_in_0 = r"C:\test\12345678\0\*.in"
            # target_path_out_0 = r"C:\test\12345678\0\*.out"
            # target_path_in_1 = r"C:\test\12345678\1\*.in"
            # target_path_out_1 = r"C:\test\12345678\1\*.out"
            # folder_in_0 = glob.glob(target_path_in_0)
            # folder_out_0 = glob.glob(target_path_out_0)
            # folder_in_1 = glob.glob(target_path_in_1)
            # folder_out_1 = glob.glob(target_path_out_1)

            # if len(folder_in_0) != len(folder_out_0) or len(folder_in_1) != len(folder_out_1):
            #     return "0 diff 1-400", 400

            if not (os.path.isfile(submission_dir / 'testcase' / 'meta.json')):
                return "no meta data-400", 400

            # read file
            with open(submission_dir / 'testcase' / 'meta.json',
                      'r') as myfile:
                data = myfile.read()
            # parse file
            obj = json.loads(data)
            value = obj['cases'][0]
            if type(value['caseScore']) == int and \
                type(value['memoryLimit']) == int and \
                type(value['timeLimit']) == int:
                logger.debug(f'send submission {submission_id} to dispatcher')
                DISPATCHER.handle(submission_id, ['c11', 'cpp11',
                                                  'python3'][language_id])
                return jsonify({'status': 'ok', 'msg': 'ok', 'data': 'ok'})
            else:
                return "none int-400", 400


def clean_data(submission_id):
    submission_dir = SUBMISSION_DIR / submission_id
    shutil.rmtree(submission_dir)


@app.route('/result/<submission_id>', methods=['POST'])
def recieve_result(submission_id):
    post_data = request.get_json()

    # convert lang to code
    tb = ['AC', 'WA', 'CE', 'TLE', 'MLE', 'RE', 'JE', 'OLE']
    post_data['status'] = tb.index(post_data['status'])
    for case in post_data['cases']:
        case['status'] = tb.index(case['status'])

    resp = requests.put(
        f'{BACKEND_API}/submission/{submission_id}?token={tokens[submission_id]}',
        json=post_data,
        cookies=cookies[submission_id])

    logger.info(f'send {submission_id} to BE server')
    logger.debug(f'send json: f{post_data}')
    logger.debug(f'cookies: f{cookies[submission_id]}')
    logger.debug(f'resp: {resp.text}')

    # clear
    del tokens[submission_id]
    del cookies[submission_id]
    if resp.status_code == 200:
        clean_data(submission_id)

    return 'data sent to BE server', 200
