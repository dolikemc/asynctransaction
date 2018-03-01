from abc import ABC, abstractmethod
from typing import List

from aiohttp.web import BaseRequest
import aiohttp

from asynctransaction.data.entity.state import *
from asynctransaction.data.entity.processing_step import ProcessingStep
from asynctransaction.data.entity.event import Event
from asynctransaction.data.entity.partner import Partner
from asynctransaction.data.entity.task import Task


class ITransaction(ABC):
    @abstractmethod
    async def receive(self, request: BaseRequest, event: Event = None) -> State:
        ...

    @abstractmethod
    async def store(self) -> State:
        ...

    @abstractmethod
    async def spread(self, task_id: int) -> State:
        ...

    @abstractmethod
    async def process(self, process: ProcessingStep, client: aiohttp.ClientSession) -> State:
        ...

    @abstractmethod
    def message(self) -> str:
        ...

    @abstractmethod
    def task(self) -> Task:
        ...


class ISubscriberAccess(ABC):
    @abstractmethod
    async def get_subscribers(self, event: int = 0):
        ...


class IEventAccess(ABC):
    @abstractmethod
    async def get_event_data(self, url: str, method: str = 'POST') -> Event:
        ...


class IPartnerAccess(ABC):
    @abstractmethod
    async def get_partner_data(self, **kwargs) -> Partner:
        ...

    @abstractmethod
    async def change_partner_data(self, **kwargs) -> Partner:
        ...


class ITaskAccess(ABC):
    @abstractmethod
    async def read_tasks(self, state: int) -> State:
        ...

    @abstractmethod
    def get_data(self) -> List:
        ...


class IProcessingStepsAccess(ABC):
    @abstractmethod
    async def read_processing_steps(self, state: int) -> State:
        ...

    @abstractmethod
    def get_data(self) -> List:
        ...
