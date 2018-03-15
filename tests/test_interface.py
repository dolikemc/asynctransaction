from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiohttp.web import Application, Response, HTTPOk
from aiohttp import ClientSession

from asynctransaction.server.distributor import *
from asynctransaction.data.access.base import prepare_connection


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
        return test_app

    async def perform_directive(self, directive: str) -> (None, str):
        if directive.startswith('       '):
            (parameter, result) = directive.split("->\n")
            if ',' in str(result):
                status = int(str(result)[0:str(result).index(',')])
                filter = str(result)[str(result).index(',') + 1:]
            else:
                status = int(result)
                filter = None
            print(parameter)
            request = await eval('self.client.request(' + parameter + ')')
            self.assertEqual(request.status, status)
            return filter

    @unittest_run_loop
    async def test_subscriber(self):
        for directive in str(SubscriberAdmin.__doc__).split("\n\n"):
            read_filter = await self.perform_directive(str(directive))
            if read_filter is None:
                continue
            subscriber = Subscriber(self.app['DISTRIBUTOR_DB'])
            await subscriber.read()
            self.assertTrue(eval("set(subscriber.data) >= set(" + read_filter + ")"))

    @unittest_run_loop
    async def test_partner(self):
        for directive in str(PartnerAdmin.__doc__).split("\n\n"):
            read_filter = await self.perform_directive(str(directive))
            if read_filter is None:
                continue
            subscriber = Partner(self.app['DISTRIBUTOR_DB'])
            await subscriber.read()
            print(set(subscriber.data))
            print(eval("set(" + read_filter + ")"))
            self.assertTrue(eval("set(subscriber.data) >= set(" + read_filter + ")"))
