from dataclasses import dataclass


@dataclass
class Compile:
    job_id: str
    # The JobContext generation this entry was enqueued under. The run loop
    # discards the entry when it is no longer the current generation, so a
    # task queued by a released or superseded handle() can never bind itself
    # to the attempt that replaced it.
    ctx: object


@dataclass
class Execute:
    job_id: str
    task_id: int
    case_id: int
    # See Compile.ctx.
    ctx: object
