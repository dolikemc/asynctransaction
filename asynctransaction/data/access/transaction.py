import json
import sqlite3
import asyncio
import logging
from typing import cast

import aiohttp
from aiohttp.web import BaseRequest

from asynctransaction.data.entity import *
from asynctransaction.data.access.transaction_if import ITransaction
from asynctransaction.data.access.base import DataAccessBase
from asynctransaction.data.access.task import Task as TaskAccess

log = logging.getLogger('asynctransaction.data.access.transaction')


class Transaction(ITransaction, DataAccessBase):
    def __init__(self, con: sqlite3.Connection):
        ITransaction.__init__(self)
        DataAccessBase.__init__(self, con=con, name='TASKS')

    async def receive(self, request: BaseRequest, this_event: Event = None) -> State:
        await super().receive(request)
        try:
            received_data: Dict = await request.json()
        except json.JSONDecodeError:
            return State.BadRequestJsonDecode
        # check for necessary data fields
        if {'PARTNER_ID', 'DATA'}.issubset(received_data.keys()) is False:
            return State.BadRequestMandatoryKey
        if this_event is None:
            if 'EVENT_ID' not in received_data:
                return State.BadRequestMandatoryKey
        else:
            received_data['EVENT_ID'] = this_event.id
            received_data['URL'] = this_event.url
            received_data['METHOD'] = this_event.method
        if isinstance(received_data['DATA'], dict):
            if 'ID' not in received_data['DATA']:
                return State.BadRequestMandatoryKey
            received_data['LOCAL_ID'] = received_data['DATA']['ID']
            received_data['DATA'] = json.dumps(received_data['DATA'])
        elif isinstance(received_data['DATA'], str) is False:
            return State.BadRequestNotStoreAble
        received_data['ID'] = 0
        try:
            self.data.append(Task(**received_data))
        except TaskException:
            return State.BadRequestNotStoreAble
        return State.RequestReceived

    async def store(self) -> State:
        await super().store()
        ta = TaskAccess(self.connection)
        future: asyncio.Future = self._loop.create_future()
        asyncio.ensure_future(ta.duplicate_check(task=self.task, future=future))
        await future
        if future.result() > 5:
            return State.ConflictRequest
        try:
            record_id = await self.insert()
        except sqlite3.Error as error:
            log.error(error)
            return State.BadRequestDBError
        for data in self.data:
            data.id = record_id
        return State.RequestStored

    async def spread(self, task_id: int) -> State:
        await super().spread(task_id)
        future: asyncio.Future = self._loop.create_future()
        asyncio.ensure_future(self._spread_task(task_id=task_id, future=future))
        await future
        return State.RequestStored

    async def process(self, process: ProcessingStep,
                      client: aiohttp.ClientSession) -> State:
        await super().process(process, client)
        ba = DataAccessBase(self.connection)
        ba.data.append(process)
        await ba.update_state(to_state=State.InProgress.code)
        try:

            url = ['http://', process.ip_address, ':',
                   str(process.port), '/transactions/', process.url]
            log.info(''.join(url))
            resp = await client.request(method=process.method, url=''.join(url),
                                        data=process.data, timeout=1.01)

            if resp.status in {200, 201}:
                await ba.update_state(to_state=State.Processed.code)
                return State.RequestStored
            else:
                await ba.update_state(to_state=State.Error.code)
                return State.BadRequest

        except aiohttp.ClientError as error:
            log.error(error)
            await ba.update_state(to_state=State.InProgress.code)
            return State.BadRequest

    @property
    def message(self) -> str:
        super().message()
        stored_task: Task = self.get_result()
        return f"/{stored_task.url}/{stored_task.local_id}/{stored_task.id}"

    @property
    def task(self) -> Task:
        super().task()
        return cast(Task, self.get_result())

    async def _spread_task(self, task_id: int, future: asyncio.Future):
        sql = """INSERT INTO PROCESSING_STEPS (TASK_ID, PARTNER_ID) SELECT TASKS.ID, SUBSCRIBERS.PARTNER_ID  
                        FROM TASKS, SUBSCRIBERS WHERE TASKS.ID = :ID AND TASKS.EVENT_ID = SUBSCRIBERS.EVENT_ID"""
        update_sql = """UPDATE TASKS SET STATE = 3, UPDATED_ON = datetime('now','localtime') WHERE ID = :ID"""
        try:
            with self.connection as con:
                con.execute(sql, {'ID': task_id})
                con.execute(update_sql, {'ID': task_id})
                future.set_result(True)
        except sqlite3.Error as error:
            future.set_exception(error)
