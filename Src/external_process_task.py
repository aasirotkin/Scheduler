#!/usr/bin/env python3

# файл чтобы появилсяинтерфейс задачи а не просто словарь TaskScec

from dataclasses import dataclass

from i_task import ITask
from task_spec import TaskSpec


@dataclass(frozen=True)
class ExternalProcessTask(ITask):
    task_id: str
    name: str
    spec: TaskSpec
    entrypoint: str

    def get_id(self) -> str:
        return self.spec.id
        
    def get_name(self) -> str:
        return self.name

    def get_spec(self) -> TaskSpec:
        return self.spec

    def get_entrypoint(self) -> str:
        return self.entrypoint
