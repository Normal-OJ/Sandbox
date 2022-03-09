from .constant import Language
from typing import List
from pydantic import (
    BaseModel,
    Field,
    validator,
    conlist,
)


class Task(BaseModel):
    taskScore: int
    memoryLimit: int
    timeLimit: int
    caseCount: int


class Meta(BaseModel):
    language: Language
    tasks: conlist(Task, min_items=1)

    @validator('tasks')
    def validate_task(cls, v):
        if sum(t.taskScore for t in v) != 100:
            raise ValueError('sum of scores must be 100')
        return v
