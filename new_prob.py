import sys
import json
from pathlib import Path

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f'Usage: python {__file__} <problem_name>')
        exit(0)
    problem_name = sys.argv[1]
    problem_root = Path('problem')
    problem_dir = problem_root / problem_name
    problem_dir.mkdir()
    (problem_dir / 'src').mkdir()
    (problem_dir / 'testcase').mkdir()
    meta = problem_dir / 'meta.json'
    json.dump({
        'language': 0,
        'tasks': [],
    }, meta.open('w'))