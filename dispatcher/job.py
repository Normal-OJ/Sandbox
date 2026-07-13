from dataclasses import dataclass


@dataclass
class Compile:
    job_id: str


@dataclass
class Execute:
    job_id: str
    task_id: int
    case_id: int
