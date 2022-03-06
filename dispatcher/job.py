from dataclasses import dataclass


@dataclass
class Compile:
    submission_id: str


@dataclass
class Execute:
    submission_id: str
    task_id: int
    case_id: int
