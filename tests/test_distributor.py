from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiohttp.web import Application, Response, HTTPOk
from aiohttp import ClientSession

from asynctransaction.server.distributor import *
from asynctransaction.data.access.base import prepare_connection
from asynctransaction.data.access.partner import Partner


# noinspection PyMissingConstructor
class TestClient(ClientSession):
    def __init__(self, response: Response):
        self.response = response

    async def request(self, method, url, **kwargs) -> Response:
        return self.response


class MyAppTestCase(AioHTTPTestCase):

    async def get_application(self):
        app = Application()
        app.middlewares.append(logger_middleware)
        app['DISTRIBUTOR_CLIENT'] = TestClient(HTTPOk())
        app['DISTRIBUTOR_DB'] = prepare_connection()
        cursor: sqlite3.Cursor = app['DISTRIBUTOR_DB']
        with open('asynctransaction/data/model/transaction.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))
        with open('asynctransaction/data/model/test_data.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))
        apply_routes(app)
        app.on_startup.append(start_background_tasks)
        app.on_cleanup.append(cleanup_background_tasks)
        return app

    # the unittest_run_loop decorator can be used in tandem with
    # the AioHTTPTestCase to simplify running
    # tests that are asynchronous
    @unittest_run_loop
    async def test_order_post(self):
        request = await self.client.request(
            "POST", "/transactions/orders",
            data='{"PARTNER_ID": 1,  "DATA": {"ID": 239, "ORDER": 12}}')
        self.assertEqual(request.status, 201)
        text = await request.text()
        self.assertEqual(text, '/orders/239/2')

    @unittest_run_loop
    async def test_bad_post(self):
        request = await self.client.request(
            "POST", "/transactions/not_exists",
            data='{"PARTNER_ID": 1,  "DATA": {"ID": 239, "ORDER": 12}}')
        self.assertEqual(request.status, 501)

    @unittest_run_loop
    async def test_bad_json(self):
        request = await self.client.request(
            "POST", "/transactions/orders",
            data='"PARTNER_ID": 1')
        self.assertEqual(request.status, 400)

    @unittest_run_loop
    async def test_bad_data(self):
        request = await self.client.request(
            "POST", "/transactions/orders",
            data='{"PARTNER_ID": 1,  "DATA": {"ORDER": 12}}')
        self.assertEqual(request.status, 400)

    @unittest_run_loop
    async def test_bad_store(self):
        request = await self.client.request(
            "POST", "/transactions/orders",
            data='{"PARTNER_ID": 100,  "DATA": {"ID": 111, "ORDER": 12}}')
        self.assertEqual(request.status, 400)

    @unittest_run_loop
    async def test_put(self):
        request = await self.client.request(
            "PUT", "/transactions/orders",
            data='{"PARTNER_ID": 1,  "DATA": {"ID": 111, "ORDER": 12}}')
        self.assertEqual(request.status, 201)
        text = await request.text()
        self.assertEqual(text, '/orders/111/2')

    @unittest_run_loop
    async def test_get(self):
        request = await self.client.request("GET", "/admin/partners/127.0.0.1:3030")
        self.assertEqual(request.status, 200)
        text = await request.text()
        self.assertIsInstance(text, str)
        self.assertTrue(text.find('IP_ADDRESS'))

    @unittest_run_loop
    async def test_only_port(self):
        request = await self.client.request("GET", "/admin/partners/3030")
        self.assertEqual(request.status, 200)

    @unittest_run_loop
    async def test_subscriber(self):
        request = await self.client.request("GET", "/admin/subscribers")
        self.assertEqual(request.status, 200)

    @unittest_run_loop
    async def test_add_partner(self):
        request = await self.client.request(
            "POST", "/admin/partners/3030",
            data={'_method': 'PUT', 'ID': '0', 'IP_ADDRESS': '127.0.0.1', 'PORT': '2',
                  'DESCRIPTION': 'CLIENT SERVER'})
        self.assertEqual(request.status, 200)
        partner = Partner(self.app['DISTRIBUTOR_DB'])
        await partner.read()
        self.assertEqual(len([a for a in filter(lambda x: x.port == 2, partner.data)]), 1)

    @unittest_run_loop
    async def test_change_partner(self):
        request = await self.client.request(
            "POST", "/admin/partners/3030",
            data={'_method': 'PUT', 'ID': '1', 'IP_ADDRESS': '127.0.0.1', 'PORT': '20',
                  'DESCRIPTION': 'CLIENT SERVER'})
        self.assertEqual(request.status, 200)
        partner = Partner(self.app['DISTRIBUTOR_DB'])
        await partner.read()
        self.assertEqual(len([a for a in filter(lambda x: x.port == 20, partner.data)]), 1)

    @unittest_run_loop
    async def test_get_not_found(self):
        request = await self.client.request("GET", "/admin/partners/127.1.1.1:3030")
        self.assertEqual(request.status, 400)

    def test_config(self):
        port = apply_config(self.app)
        self.assertEqual(port, 3010)
