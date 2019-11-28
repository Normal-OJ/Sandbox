import docker
from time import sleep

class submission_runner():
	"""docstring for container"""
	def __init__(self, submission_id, time_limit, mem_limit, special_judge=False, image='ubuntu:18.04', lang='python3'):
		# docker
		self.client = docker.from_env()
		self.image = image
		# optional
		self.lang = lang
		self.special_judge = special_judge
		# required
		self.submission_id = submission_id
		self.time_limit = time_limit
		self.mem_limit = mem_limit
	def c_compile(self):
		compile_argument = 'gcc -DONLINE_JUDGE -O2 -Wall -fmax-errors=3 -std=c11 main.c -lm -o main'
		working_dir = '/submission'
		volume = {f'/home/as535364/桌面/docker-playground/submissions/{self.submission_id}': {'bind': '/submission', 'mode': 'rw'}}
		container = self.client.containers.run(image='gcc', command=compile_argument, detach=True, volumes=volume, network_disabled=True, working_dir=working_dir)
		container.wait(timeout=5*self.time_limit/1000)
		stdout = container.logs(stdout=True, stderr=False).decode('utf-8')
		stderr = container.logs(stdout=False, stderr=True).decode('utf-8')
		container.remove()
		return stdout, stderr

	def c_run(self):
		execute = './main'
		working_dir = '/submission'
		volume = {f'/home/as535364/桌面/docker-playground/submissions/{self.submission_id}': {'bind': '/submission', 'mode': 'rw'}}
		container = self.client.containers.run(image=self.image, command=execute, detach=True, volumes=volume, network_disabled=True, working_dir=working_dir)
		container.wait(timeout=5*self.time_limit/1000)
		stdout = container.logs(stdout=True, stderr=False).decode('utf-8')
		stderr = container.logs(stdout=False, stderr=True).decode('utf-8')
		container.remove()
		return stdout, stderr

	def python_run(self):
		execute = 'python3 -u main.py'
		working_dir = '/submission'
		volume = {f'/home/as535364/桌面/docker-playground/submissions/{self.submission_id}': {'bind': '/submission', 'mode': 'rw'}}
		container = self.client.containers.run(image='python', command=execute, detach=True,cpu_quota=self.time_limit, volumes=volume, network_disabled=True, working_dir=working_dir)
		container.wait(timeout=5*self.time_limit/1000)
		stdout = container.logs(stdout=True, stderr=False).decode('utf-8')
		stderr = container.logs(stdout=False, stderr=True).decode('utf-8')
		container.remove()
		return stdout, stderr

