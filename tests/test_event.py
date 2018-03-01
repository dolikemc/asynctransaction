from nose import tools as nt
from typing import Dict
import sqlite3
import asyncio

from aiohttp.test_utils import unittest_run_loop

from asynctransaction.data.entity.event import Event
from asynctransaction.data.access.event import Event as EventAccess
from asynctransaction.data.access.base import prepare_connection


class TestEvent:
    def __init__(self):
        self.record: Dict = {}
        self.dbh: sqlite3.Connection = None
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()

    def setup(self):
        self.record = {'ID': 1, 'URL': 'orders', 'METHOD': 'POST'}
        self.dbh = prepare_connection()
        cursor: sqlite3.Cursor = self.dbh.cursor()
        with open('asynctransaction/data/model/transaction.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))
        with open('asynctransaction/data/model/test_data.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))

    def teardown(self):
        self.dbh.close()

    def test_ctor(self):
        ev = Event()
        nt.assert_equal(ev.id, 0)
        nt.assert_equal(ev.method, 'POST')
        nt.assert_equal(ev.name, 'EVENTS')

    def test_ctor_record(self):
        ev = Event(**self.record)
        nt.assert_equal(ev.id, 1)
        nt.assert_equal(ev.method, 'POST')
        nt.assert_equal(ev.name, 'EVENTS')
        nt.assert_equal(ev.url, 'orders')

    def test_to_dict(self):
        ev = Event(**self.record)
        nt.assert_dict_contains_subset(self.record, ev.to_dict())
        nt.assert_dict_contains_subset({'DELETED': 0, 'ID': 1}, ev.to_dict())

    @unittest_run_loop
    async def test_event_access(self):
        ev = EventAccess(self.dbh)
        data = await ev.get_event_data(url='orders', method='POST')
        nt.assert_equals(data.id, 1)
        with nt.assert_raises(IndexError):
            await ev.get_event_data(url='order_drops', method='POST')
        with nt.assert_raises(IndexError):
            await ev.get_event_data(url='orders', method='GET')
