from aiohttp.test_utils import AioHTTPTestCase
from aiohttp.web import Application, HTTPOk, Response
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
        test_app.on_startup.append(start_background_tasks)
        test_app.on_cleanup.append(cleanup_background_tasks)
        return test_app
