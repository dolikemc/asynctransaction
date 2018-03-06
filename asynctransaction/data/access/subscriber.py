import sqlite3
from typing import cast, List, Dict

from asynctransaction.data.access.base import DataAccessBase
from asynctransaction.data.access.transaction_if import ISubscriberAccess
from asynctransaction.data.entity.subscriber import Subscriber as SubscriberEntity


class Subscriber(DataAccessBase, ISubscriberAccess):
    def __init__(self, con: sqlite3.Connection):
        DataAccessBase.__init__(self, con=con, name='SUBSCRIBERS')

    async def get_subscribers(self, event: int = 0) -> List[Dict]:
        await super().get_subscribers(event)
        self.data.clear()
        await self.read(entity_id=0, no_join=False)

        return [x.to_dict() for x in self.data]

    def get_result(self, uniqueness_expected: bool = True):
        return cast(SubscriberEntity, super().get_result(uniqueness_expected))
