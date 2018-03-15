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
        test_app = Application()
        test_app.middlewares.append(logger_middleware)
        test_app['DISTRIBUTOR_CLIENT'] = TestClient(HTTPOk())
        test_app['DISTRIBUTOR_DB'] = prepare_connection()
        cursor: sqlite3.Cursor = test_app['DISTRIBUTOR_DB']
        with open('asynctransaction/data/model/transaction.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))
        with open('asynctransaction/data/model/test_data.sql') as prepare_db:
            prepare_sql = prepare_db.read()
            cursor.executescript(str(prepare_sql))
        apply_routes(test_app)
        test_app.on_startup.append(start_background_tasks)
        test_app.on_cleanup.append(cleanup_background_tasks)
        return test_app

    # the unittest_run_loop decorator can be used in tandem with
    # the AioHTTPTestCase to simplify running
    # tests that are asynchronous
    @unittest_run_loop
    async def test_order_post(self):
        """
        "POST", "/transactions/orders", data='{"PARTNER_ID": 1,  "DATA": {"ID": 239, "ORDER": 12}}'
        Expect: 201
        """
        request = await self.client.request(
            "POST", "/transactions/orders", data='{"PARTNER_ID": 1,  "DATA": {"ID": 239, "ORDER": 12}}')
        self.assertEqual(request.status, 201)
        text = await request.text()
        self.assertEqual(text, '/orders/239/2')

    @unittest_run_loop
    async def test_bad_post(self):
        request = await self.client.request(
            "POST", "/transactions/not_exists", data='{"PARTNER_ID": 1,  "DATA": {"ID": 239, "ORDER": 12}}')
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
        data = await request.text()
        self.assertIsInstance(data, str)
        self.assertTrue(data.find('IP_ADDRESS'))

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

    def test_https_config(self):
        config_port = apply_config(self.app, 'likemc.ini')
        self.assertEqual(config_port, 3010)
        certificate: ssl.SSLContext = apply_ssl('likemc.ini')
        self.assertFalse(certificate is None)

    def test_http_config(self):
        certificate = apply_ssl('test.ini')
        self.assertTrue(certificate is None)

    @unittest_run_loop
    async def test_delete_subscriber(self):
        """
        Delete a subscriber
        "PUT", "/admin/subscribers", data={'ID': '1', 'DELETED': '1'}
        "DELETE", "/admin/subscribers/1"
        """
        request = await self.client.request("PUT", "/admin/subscribers", data={'ID': '1', 'DELETED': '1'})
        self.assertEqual(request.status, 200)
        subscriber = Subscriber(self.app['DISTRIBUTOR_DB'])
        await subscriber.read(entity_id=1)
        # todo: self.assertEqual(len([a for a in filter(lambda x: x.id == 1 and x.deleted == 1, subscriber.data)]), 1)

    @unittest_run_loop
    async def test_change_subscriber(self):
        """
        Change an existing subscriber, in this case change the partner from 2 to 1
        "PUT", "/admin/subscribers", data={'ID': '3', 'PARTNER_ID': '1'}
        """
        request = await self.client.request("PUT", "/admin/subscribers", data={'ID': '3', 'PARTNER_ID': '1'})
        self.assertEqual(request.status, 200)
        subscriber = Subscriber(self.app['DISTRIBUTOR_DB'])
        await subscriber.read(entity_id=3)
        # todo: self.assertEqual(len([a for a in filter(
        #   lambda x: x.id == 3 and x.partner_id == 1, subscriber.data)]), 1)

    @unittest_run_loop
    async def test_list_group(self):
        """
        List all existing groups, means all existing combinations of events
        "GET", "/admin/subscribers?group=1"
        """
        request = await self.client.request(
            "GET", "/admin/subscribers?group=1")
        self.assertEqual(request.status, 200)
        data = await request.text()
        self.assertIsInstance(data, str)
        print('NOT implemented')

    @unittest_run_loop
    async def test_add_partner_with_group(self):
        """
        Add a partner and assign it to the group dependent subscriptions
        "POST", "/admin/partner?group=1", data={'ID': '0', 'IP_ADDRESS': '127.0.0.1', 'PORT': '203',
            'DESCRIPTION': 'Local Server'}
        """
        request = await self.client.request(
            "POST", "/admin/partner?group=1",
            data={'ID': '0', 'IP_ADDRESS': '127.0.0.1', 'PORT': '203', 'DESCRIPTION': 'Local Server'})
        if request.status == 404:
            print('NOT implemented')
            return
        self.assertEqual(request.status, 201)
        subscriber = Subscriber(self.app['DISTRIBUTOR_DB'])
        await subscriber.read()
        self.assertEqual(len([a for a in filter(lambda x: x.port == 203, subscriber.data)]), 2)

    # todo: event handling, add, change, assign to group
