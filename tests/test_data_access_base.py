import nose.tools as nt
import asyncio
import sqlite3

from aiohttp.test_utils import unittest_run_loop

from asynctransaction.data.access.factory import *
from asynctransaction.data.access.base import DataAccessBase, MixedEntitiesException, prepare_connection
from asynctransaction.data.entity import *


class TestDataAccessBase:
    def __init__(self):
        self.record: Dict = {}
        self.dbh = None
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    def setup(self):
        self.record = {'ID': 1, 'URL': 'orders', 'METHOD': 'POST'}
        self.dbh = prepare_connection()

    def teardown(self):
        self.dbh.close()

    def test_ctor(self):
        test = DataAccessBase(self.dbh)
        nt.assert_equal(test.name, '')

    def test_bad_entity_factory(self):
        dac = DataAccessBase(con=self.dbh, name='DOES_NOT_EXISTS', loop=self.loop)
        with nt.assert_raises(NoValidEntity):
            # noinspection PyProtectedMember
            dac._entity_factory(self.record)

    def test_bad_entities(self):
        dac = DataAccessBase(con=self.dbh, name='', loop=self.loop)
        dac.data.append(Task())
        dac.data.append(Event())
        with nt.assert_raises(MixedEntitiesException):
            dac.name()
        with nt.assert_raises(IndexError):
            dac.get_result()

    @unittest_run_loop
    async def test_bad_insert(self):
        dac = DataAccessBase(con=self.dbh, name='EVENTS')
        nt.assert_equals(await dac.insert(), 0)

    @unittest_run_loop
    async def test_bad_entity(self):
        dac = DataAccessBase(con=self.dbh, name='XXX')
        nt.assert_equals(await dac.update_state(0), 0)
        nt.assert_equals(await dac.read(), 0)

    def test_bad_factory(self):
        with nt.assert_raises(NotImplementedError):
            create_transaction(self.dbh, 'x')
        with nt.assert_raises(NotImplementedError):
            create_partner_access(self.dbh, 'x')
        with nt.assert_raises(NotImplementedError):
            create_event_access(self.dbh, 'x')
        with nt.assert_raises(NotImplementedError):
            create_task_access(self.dbh, 'x')
        with nt.assert_raises(NotImplementedError):
            create_processing_step_access(self.dbh, 'x')
        with nt.assert_raises(NotImplementedError):
            create_subscriber_access(self.dbh, 'x')

    # noinspection PyProtectedMember
    @unittest_run_loop
    async def test_bad_execute(self):
        dac = DataAccessBase(con=self.dbh, name='EVENTS')
        future: asyncio.Future = self.loop.create_future()
        with nt.assert_raises(sqlite3.DatabaseError):
            await dac._execute_select(sql='SELECT * FROM X', parameters=[], future=future)
            await future
        with nt.assert_raises(asyncio.InvalidStateError):
            await dac._execute_select(sql='SELECT * FROM TASKS WHERE ID = ?', parameters=[1], future=future)
            future.cancel()
