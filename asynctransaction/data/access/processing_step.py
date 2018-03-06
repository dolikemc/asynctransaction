import sqlite3
from typing import List, cast

from asynctransaction.data.entity.state import *
from asynctransaction.data.access.base import DataAccessBase
from asynctransaction.data.access.transaction_if import IProcessingStepsAccess


class ProcessingStep(DataAccessBase, IProcessingStepsAccess):
    def __init__(self, con: sqlite3.Connection):
        DataAccessBase.__init__(self, con=con, name='PROCESSING_STEPS')

    async def read_processing_steps(self, state: int) -> State:
        await super().read_processing_steps(state)
        await self.read(entity_id=state, no_join=False, by_state=True)
        return State.RequestStored

    def get_data(self) -> List:
        super().get_data()
        return self.data

    def get_result(self, uniqueness_expected: bool = True):
        return cast(ProcessingStep, super().get_result())
