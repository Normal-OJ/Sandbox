import os
import json


class ProblemParser:
    def __init__(self, data_path='problem'):
        super().__init__()

        if not os.path.exists(data_path):
            raise FileNotFoundError(data_path)
        if not os.path.isdir(data_path):
            raise NotADirectoryError(f'{data_path} is not a directory')

        self.data_path = data_path
        # Dict[problem_name, problem_data]
        self.problem = {}

    def parse(self):
        for prob in os.listdir(self.data_path):
            self.problem[prob] = {}
            prob_data = self.problem[prob]
            prob_base_dir = f'{self.data_path}/{prob}'

            # read metadata
            with open(f'{prob_base_dir}/prob.json') as f:
                prob_data['meta'] = json.load(f)

            with open(f'{prob_base_dir}/testcase/meta.json') as f:
                prob_data['meta'].update(json.load(f))

            # parse source code
            prob_data['source'] = {}
            for src in os.listdir(f'{prob_base_dir}/src'):
                with open(f'{prob_base_dir}/src/{src}') as f:
                    prob_data['source'][src] = f.read()

            # read testcase
            prob_data['testcase'] = []
            for i in range(len(prob_data['meta']['cases'])):
                with open(f'{prob_base_dir}/testcase/{i}/in') as f:
                    t_in = f.read()

                with open(f'{prob_base_dir}/testcase/{i}/out') as f:
                    t_out = f.read()

                prob_data['testcase'].append({'in': t_in, 'out': t_out})
        return self.problem
