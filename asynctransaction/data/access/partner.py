import sqlite3
import asyncio
import logging
from ipaddress import IPv4Address
from typing import cast

from asynctransaction.data.access.base import DataAccessBase
from asynctransaction.data.access.transaction_if import IPartnerAccess
from asynctransaction.data.entity.partner import Partner as PartnerEntity

log = logging.getLogger('asynctransaction.data.access.partner')


class Partner(DataAccessBase, IPartnerAccess):
    def __init__(self, con: sqlite3.Connection):
        DataAccessBase.__init__(self, con=con, name='PARTNERS')

    async def get_partner_data(self, **kwargs) -> PartnerEntity:
        await super().get_partner_data(**kwargs)
        partner = PartnerEntity(**kwargs)
        self.data.clear()
        future: asyncio.Future = self.loop.create_future()
        asyncio.ensure_future(self._read_partner_by_address(
            ip_address=partner.ip_address, port=partner.port, future=future))
        await future
        return cast(PartnerEntity, self.get_result())

    async def _read_partner_by_address(self, ip_address: IPv4Address, port: int, future: asyncio.Future):
        parameters = {'IP_ADDRESS': str(ip_address), 'PORT': port}
        log.debug(parameters)
        sql = f'SELECT * FROM {self.name} WHERE ip_address = :IP_ADDRESS AND port = :PORT AND DELETED = 0'
        for row in self.connection.execute(sql, parameters):
            self.data.append(PartnerEntity(**row))
        future.set_result(len(self.data))

    async def change_partner_data(self, **kwargs) -> PartnerEntity:
        await super().change_partner_data(**kwargs)
        partner = PartnerEntity(**kwargs)
        log.info(partner.to_dict())
        self.data.clear()
        self.data.append(partner)
        if partner.id == 0:  # insert
            partner.id = await self.insert()
        else:  # update
            sql = self.get_result().create_update_statement(**partner.to_dict())
            future: asyncio.Future = self.loop.create_future()
            asyncio.ensure_future(self._execute_one(sql, [x.to_dict() for x in self.data], future))
            await future
            await self.read(entity_id=partner.id)
        return cast(PartnerEntity, self.get_result())
