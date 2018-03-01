import sqlite3
import asyncio
from typing import cast

from asynctransaction.data.access.base import DataAccessBase
from asynctransaction.data.access.transaction_if import IEventAccess
from asynctransaction.data.entity.event import Event as EventEntity


class Event(DataAccessBase, IEventAccess):
    def __init__(self, con: sqlite3.Connection):
        DataAccessBase.__init__(self, con=con, name='EVENTS')

    async def get_event_data(self, url: str, method: str = 'POST') -> EventEntity:
        await super().get_event_data(url, method)
        self.data.clear()
        future: asyncio.Future = self.loop.create_future()
        asyncio.ensure_future(self._read_by_url(url, method, future))
        await future
        return cast(EventEntity, self.get_result())

    async def _read_by_url(self, url: str, method: str, future: asyncio.Future):
        sql = f'SELECT * FROM {self.name} WHERE url = :URL AND method = :METHOD AND DELETED = 0'
        for row in self.connection.execute(sql, {'URL': url, 'METHOD': method}):
            self.data.append(EventEntity(**row))
        future.set_result(len(self.data))
