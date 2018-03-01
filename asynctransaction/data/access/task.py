import sqlite3
import asyncio
from datetime import datetime, timedelta
from typing import List, cast
import logging

from asynctransaction.data.access.transaction_if import ITaskAccess
from asynctransaction.data.access.base import DataAccessBase
from asynctransaction.data.entity.state import *
from asynctransaction.data.entity.task import Task as TaskEntity

log = logging.getLogger('asynctransaction.data.access.task')


class Task(DataAccessBase, ITaskAccess):
    def __init__(self, con: sqlite3.Connection):
        DataAccessBase.__init__(self, con=con, name='TASKS')

    async def duplicate_check(self, task: TaskEntity, future: asyncio.Future):
        credibly = 0
        sql = """SELECT TASKS.ID, TASKS.STATE, TASKS.UPDATED_ON, EV.METHOD 
                   FROM TASKS, EVENTS EV  
                  WHERE TASKS.EVENT_ID = :EVENT_ID AND PARTNER_ID = :PARTNER_ID  
                    AND TASKS.EVENT_ID = EV.ID AND LOCAL_ID = :LOCAL_ID"""
        for row in self.connection.execute(sql, task.to_dict()):
            if row['METHOD'] == 'POST':
                return future.set_result(100)
            if row['STATE'] in {3, 4}:  # already processed, no problem also if it's a duplicate
                continue
            if row['STATE'] == 5:  # same data once on error, could be a problem
                log.error("Error in the last put")
                credibly += 1
                continue
            if datetime.now() - row['UPDATED_ON'] < timedelta(minutes=2):
                log.warning(f"{row['ID']} could be a duplicate")
                credibly += 2
                continue
        future.set_result(credibly)

    async def read_tasks(self, state: int) -> State:
        await super().read_tasks(state)
        sql = f"SELECT * FROM {self.name} WHERE STATE = ?"
        future: asyncio.Future = self.loop.create_future()
        asyncio.ensure_future(self._execute_select(sql, [state], future))
        await future
        return State.RequestStored

    def get_data(self) -> List:
        super().get_data()
        return self.data

    def get_result(self, uniqueness_expected: bool = True):
        return cast(Task, super().get_result(uniqueness_expected))
