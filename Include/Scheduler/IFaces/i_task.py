#!/usr/bin/env python3

from abc import ABC, abstractmethod

from Include.Scheduler.IFaces.task_spec import TaskSpec


class ITask(ABC):
    @abstractmethod
    def get_id(self) -> str:
        ...

    @abstractmethod
    def get_name(self) -> str:
        ...

    @abstractmethod
    def get_spec(self) -> TaskSpec:
        ...

    @abstractmethod
    def get_entrypoint(self) -> str:
        ...
