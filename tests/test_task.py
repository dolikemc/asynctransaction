from nose import tools as nt
from typing import Dict
from datetime import datetime
import json
import asyncio
import sqlite3

from aiohttp.test_utils import unittest_run_loop

from asynctransaction.data.access.base import prepare_connection
from asynctransaction.data.entity.task import Task
from asynctransaction.data.access.task import Task as TaskAccess


class TestTask:
    def __init__(self):
        self.data: Dict = {}
        self.record: Dict = {}
        self.dbh: sqlite3.Connection = None
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()

    def setup(self):
        self.data = {'ID': 342, 'ORDER': 'LM324N', 'QTY': 70}
        self.record = {'ID': 238, 'PARTNER_ID': 2, 'EVENT_ID': 3, 'STATE': 1,
                       'DATA': json.dumps(self.data)}
        self.dbh = prepare_connection()
        cursor: sqlite3.Cursor = self.dbh.cursor()
        with open('asynctransaction/data/model/transaction.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))
        with open('asynctransaction/data/model/test_data.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))

    def teardown(self):
        pass

    def test_ctor(self):
        task = Task()
        nt.assert_equal(task.name, 'TASKS')
        nt.assert_equal(task.id, 0)
        nt.assert_equal(task.deleted, False)
        nt.assert_less_equal(task.created_on, datetime.now())

    def test_ctor_with_record(self):
        task = Task(**self.record)
        nt.assert_equal(task.name, 'TASKS')
        nt.assert_equal(task.id, self.record['ID'])
        nt.assert_equal(task.local_id, self.data['ID'])

    def test_create_table(self):
        expected = """CREATE TABLE TASKS (
    ID INTEGER PRIMARY KEY,
    LOCAL_ID INTEGER ,
    PARTNER_ID INTEGER ,
    EVENT_ID INTEGER ,
    DATA TEXT ,
    STATE INTEGER DEFAULT(1),
    UPDATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    CREATED_ON TIMESTAMP DEFAULT(datetime('now','localtime')),
    DELETED INTEGER DEFAULT(0),
    CONSTRAINT FK_TASKS_PARTNER_ID foreign key(PARTNER_ID) REFERENCES PARTNERS(ID),
    CONSTRAINT FK_TASKS_EVENT_ID foreign key(EVENT_ID) REFERENCES EVENTS(ID));"""
        nt.assert_multi_line_equal(Task().create_table_statement(), expected)

    @unittest_run_loop
    async def test_task_access_really_new(self):
        ta = TaskAccess(self.dbh)
        task = Task(**self.record)
        ta.data.append(task)
        future: asyncio.Future = self.loop.create_future()
        asyncio.ensure_future(ta.duplicate_check(task=ta.get_result(), future=future))
        await future
        nt.assert_equals(future.result(), 0)

    @unittest_run_loop
    async def test_task_access_duplicate(self):
        ta = TaskAccess(self.dbh)
        task = Task(**{'LOCAL_ID': 238, 'PARTNER_ID': 1, 'EVENT_ID': 1})
        ta.data.append(task)
        future: asyncio.Future = self.loop.create_future()
        asyncio.ensure_future(ta.duplicate_check(task=ta.get_result(), future=future))
        await future
        nt.assert_equals(future.result(), 100)

    @unittest_run_loop
    async def test_task_access_put_processed(self):
        ta = TaskAccess(self.dbh)
        task = Task(**{'LOCAL_ID': 237, 'PARTNER_ID': 1, 'EVENT_ID': 2})
        self.dbh.execute('INSERT INTO TASKS (LOCAL_ID, PARTNER_ID, EVENT_ID, STATE) VALUES (237, 1, 2, 3)')
        ta.data.append(task)
        future: asyncio.Future = self.loop.create_future()
        asyncio.ensure_future(ta.duplicate_check(task=ta.get_result(), future=future))
        await future
        nt.assert_equals(future.result(), 0)

    @unittest_run_loop
    async def test_task_access_put_with_error(self):
        ta = TaskAccess(self.dbh)
        task = Task(**{'LOCAL_ID': 237, 'PARTNER_ID': 1, 'EVENT_ID': 2})
        self.dbh.execute('INSERT INTO TASKS (LOCAL_ID, PARTNER_ID, EVENT_ID, STATE) VALUES (237, 1, 2, 5)')
        ta.data.append(task)
        future: asyncio.Future = self.loop.create_future()
        asyncio.ensure_future(ta.duplicate_check(task=ta.get_result(), future=future))
        await future
        nt.assert_equals(future.result(), 1)

    @unittest_run_loop
    async def test_task_access_put_high_probability(self):
        ta = TaskAccess(self.dbh)
        task = Task(**{'LOCAL_ID': 237, 'PARTNER_ID': 1, 'EVENT_ID': 2})
        self.dbh.execute('INSERT INTO TASKS (LOCAL_ID, PARTNER_ID, EVENT_ID, STATE) VALUES (237, 1, 2, 5)')
        self.dbh.execute('INSERT INTO TASKS (LOCAL_ID, PARTNER_ID, EVENT_ID, STATE) VALUES (237, 1, 2, 1)')
        ta.data.append(task)
        future: asyncio.Future = self.loop.create_future()
        asyncio.ensure_future(ta.duplicate_check(task=ta.get_result(), future=future))
        await future
        nt.assert_equals(future.result(), 3)

    @unittest_run_loop
    async def test_task_access_old_put(self):
        ta = TaskAccess(self.dbh)
        task = Task(**{'LOCAL_ID': 237, 'PARTNER_ID': 1, 'EVENT_ID': 2})
        self.dbh.execute('INSERT INTO TASKS (LOCAL_ID, PARTNER_ID, EVENT_ID, STATE, UPDATED_ON) ' +
                         'VALUES (237, 1, 2, 1, :date)', {'date': datetime(2011, 1, 1, 12, 12, 13)})
        ta.data.append(task)
        future: asyncio.Future = self.loop.create_future()
        asyncio.ensure_future(ta.duplicate_check(task=ta.get_result(), future=future))
        await future
        nt.assert_equals(future.result(), 0)
