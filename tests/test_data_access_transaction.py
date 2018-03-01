import nose.tools as nt
import json
import sqlite3
import asyncio

from aiohttp.test_utils import unittest_run_loop
from aiohttp import web
from aiohttp import ClientSession

from asynctransaction.data.access.base import prepare_connection, DataAccessBase
from asynctransaction.data.access.transaction import Transaction
from asynctransaction.data.access.processing_step import ProcessingStep as ProcessingAccess
from asynctransaction.data.entity import *


# noinspection PyMissingConstructor
class TestClient(ClientSession):
    def __init__(self, response: web.Response):
        self.response = response

    async def request(self, method, url, **kwargs) -> web.Response:
        return self.response


class TestRequest(web.BaseRequest):
    # noinspection PyMissingConstructor
    def __init__(self, data):
        self.data = data

    async def json(self, *, loads=json.loads):
        return self.data


class TestTransaction:
    def __init__(self):
        self.dbh: sqlite3.Connection = None
        self.tc: Transaction = None
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    def setup(self):
        self.dbh = prepare_connection()
        cursor: sqlite3.Cursor = self.dbh.cursor()
        with open('asynctransaction/data/model/transaction.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))
        with open('asynctransaction/data/model/test_data.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))
        self.tc = Transaction(self.dbh)

    def teardown(self):
        self.dbh.close()

    def test_ctor(self):
        nt.assert_true(isinstance(self.tc.data, list))
        nt.assert_equal(len(self.tc.data), 0)

    @unittest_run_loop
    async def test_bad_receive_bad_body(self):
        nt.assert_equal(await self.tc.receive(TestRequest(data={})), State.BadRequestMandatoryKey)

    @unittest_run_loop
    async def test_bad_receive_no_data(self):
        data = {'DATA': {}, 'PARTNER_ID': 1, 'EVENT_ID': 23}
        nt.assert_equal(await self.tc.receive(TestRequest(data=data)), State.BadRequestMandatoryKey)

    @unittest_run_loop
    async def test_bad_receive_str_data(self):
        data = 'DATA'
        with nt.assert_raises(AttributeError):
            await self.tc.receive(TestRequest(data=data))

    @unittest_run_loop
    async def test_bad_receive_bad_data_format(self):
        data = {'DATA': 3001, 'PARTNER_ID': 1, 'EVENT_ID': 23}
        nt.assert_equal(await self.tc.receive(TestRequest(data=data)), State.BadRequestNotStoreAble)

    @unittest_run_loop
    async def test_bad_receive_wrong_data_type(self):
        data = {'DATA': 'text', 'PARTNER_ID': 1, 'EVENT_ID': 23}
        nt.assert_equal(await self.tc.receive(TestRequest(data=data)), State.BadRequestNotStoreAble)

    @unittest_run_loop
    async def test_receive(self):
        data = {'DATA': {'ID': 3876, 'DATA': 'important things'}, 'PARTNER_ID': 1, 'EVENT_ID': 23}
        nt.assert_equal(await self.tc.receive(TestRequest(data=data)), State.RequestReceived)

    @unittest_run_loop
    async def test_bad_event_receive(self):
        data = {'DATA': {'ID': 3876, 'DATA': 'important things'}, 'PARTNER_ID': 1}
        nt.assert_equal(await self.tc.receive(TestRequest(data=data)), State.BadRequestMandatoryKey)

    @unittest_run_loop
    async def test_receive_with_event(self):
        this_event = Event(**{'ID': 1})
        data = {'DATA': {'ID': 3876, 'DATA': 'important things'}, 'PARTNER_ID': 1}
        nt.assert_equal(await self.tc.receive(TestRequest(data=data), this_event=this_event),
                        State.RequestReceived)

    @unittest_run_loop
    async def test_bad_store_db_error(self):
        data = {'LOCAL_ID': 238, 'PARTNER_ID': 100, 'EVENT_ID': 100,
                'DATA': json.dumps({'ID': 238, 'DATA': 'confuser cat'})}
        self.tc.data.clear()
        self.tc.data.append(Task(**data))
        nt.assert_equal(await self.tc.store(), State.BadRequestDBError)

    @unittest_run_loop
    async def test_bad_store_conflict(self):
        data = {'LOCAL_ID': 238, 'PARTNER_ID': 1, 'EVENT_ID': 1,
                'DATA': json.dumps({'ID': 238, 'DATA': 'confuser cat'})}
        self.tc.data.clear()
        self.tc.data.append(Task(**data))
        nt.assert_equal(await self.tc.store(), State.ConflictRequest)

    @unittest_run_loop
    async def test_store(self):
        data = {'LOCAL_ID': 238, 'PARTNER_ID': 1, 'EVENT_ID': 2,
                'DATA': json.dumps({'ID': 238, 'DATA': 'confuser cat update'})}
        self.tc.data.clear()
        self.tc.data.append(Task(**data))
        nt.assert_equal(await self.tc.store(), State.RequestStored)

    @unittest_run_loop
    async def test_spread(self):
        for row in self.dbh.execute("SELECT ID FROM TASKS WHERE STATE=1"):
            nt.assert_equal(await self.tc.spread(row['ID']), State.RequestStored)

    @unittest_run_loop
    async def test_bad_spread(self):
        self.dbh.execute('DROP TABLE PROCESSING_STEPS')
        self.dbh.commit()
        with nt.assert_raises(sqlite3.Error):
            await self.tc.spread(task_id=1)

    @unittest_run_loop
    async def test_datetime(self):
        dac = DataAccessBase(self.dbh, 'TASKS')
        await dac.read()
        for row in dac.data:
            nt.assert_is_instance(row.updated_on, datetime)

    @unittest_run_loop
    async def test_read(self):
        dac = DataAccessBase(self.dbh, 'PARTNERS')
        await dac.read()
        nt.assert_equal(len(dac.data), 2)
        for record in dac.data:
            nt.assert_equal(record.name, 'PARTNERS')
            nt.assert_greater(record.id, 0)

    @unittest_run_loop
    async def test_read_joined_tasks(self):
        dac = DataAccessBase(self.dbh, 'TASKS')
        await dac.read()
        nt.assert_equal(len(dac.data), 1)

    @unittest_run_loop
    async def test_read_joined_processing_steps(self):
        dac = DataAccessBase(self.dbh, 'PROCESSING_STEPS')
        await dac.read(entity_id=1)
        nt.assert_equal(len(dac.data), 1)

    @unittest_run_loop
    async def test_read_joined_by_id(self):
        dac = DataAccessBase(self.dbh, 'TASKS')
        await dac.read(entity_id=1)
        nt.assert_equal(len(dac.data), 1)

    @unittest_run_loop
    async def test_update_state(self):
        dac = ProcessingAccess(self.dbh)
        nt.assert_equal(await dac.update_state(0), 0)
        dac.data.clear()
        dac.data.append(ProcessingStep(ID=1))
        await dac.read(entity_id=1, no_join=True)
        nt.assert_greater_equal(len(dac.data), 1)
        state_before = dac.get_result(False).state
        nt.assert_equal(await dac.update_state(2), 1)
        await dac.read(entity_id=1, no_join=True)
        nt.assert_equal(dac.get_result(False).state, state_before + 1)
        nt.assert_equal(await dac.update_state(to_state=5), 1)
        await dac.read(entity_id=1, no_join=True)
        nt.assert_equal(dac.get_result(False).state, 5)

    @unittest_run_loop
    async def test_process(self):
        dac = ProcessingAccess(self.dbh)
        await dac.read(entity_id=1)
        nt.assert_greater_equal(len(dac.data), 1)
        client = TestClient(web.HTTPOk())
        result = await self.tc.process(dac.get_result(), client)
        nt.assert_equal(result, State.RequestStored)

    @unittest_run_loop
    async def test_bad_process(self):
        dac = ProcessingAccess(self.dbh)
        await dac.read(entity_id=1)
        nt.assert_greater_equal(len(dac.data), 1)
        client = TestClient(web.HTTPBadRequest())
        nt.assert_equal(await self.tc.process(dac.get_result(), client), State.BadRequest)

    @unittest_run_loop
    async def test_no_client_connection(self):
        dac = ProcessingAccess(self.dbh)
        await dac.read(entity_id=1)
        nt.assert_greater_equal(len(dac.data), 1)
        client = ClientSession(loop=self.loop, conn_timeout=1.0)
        nt.assert_equal(await self.tc.process(dac.get_result(), client), State.BadRequest)
