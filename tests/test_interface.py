import doctest
from tests.restapi import MyAppTestCase, TestClient
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiohttp.web import Application, Response, HTTPOk


class DocMyAppTestCase(MyAppTestCase):
    @unittest_run_loop
    async def test_base(self):
        response = await self.client.get('/admin/partners/3010')
        result = await response.text()
        self.assertTrue(result)
