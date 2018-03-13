""":module: Distribution

Secure Connection
=================

*Create a certificate*

    openssl req -new -x509 -keyout server.pem -out server.pem -days 365 -nodes

*Get via https*

    curl -v --insecure https://localhost:3010/admin/partners/127.0.0.1:3030
    curl -v --cacert server.pem https://localhost:3010/admin/partners/127.0.0.1:3030

*Put via https*

    curl -v --cacert server.pem --data '{"PARTNER_ID": 1,  "DATA": {"ID": 239, "ORDER": 12}}' \
-X PUT https://localhost:3010/transactions/orders

"""

import sqlite3
from configparser import ConfigParser
from datetime import datetime
from typing import Dict
import asyncio
import logging
import ssl

from aiohttp import web
import aiohttp_jinja2
from jinja2 import FileSystemLoader

from asynctransaction.data.access.factory import *
from asynctransaction.data.access.base import prepare_connection

__version__ = '0.5.0'
CONFIG_FILE_NAME: str = 'likemc.ini'

log = logging.getLogger('asynctransaction.server.distributor')
logging.basicConfig(level=logging.DEBUG)


class SubscriberAdmin(web.View):
    """Subscriber Admin Screen API.

    * Method    URL                         DATA                                CODE    RESULT DATA
    * PUT       /admin/subscribers          data={'ID': '1', 'DELETED': '1'}    200     {'ID': 1, 'DELETED': 1}
    * PUT       /admin/subscribers          data={'ID': 1, 'DELETED': 1}        200     {'ID': 1, 'DELETED': 1}
    * DELETE    /admin/subscribers/1        data=None                           200     {'ID': 1, 'DELETED': 1}
    * PUT       /admin/subscribers          data={'ID': 3, 'PARTNER_ID': 1}     200     {'ID': 3, 'PARTNER_ID': 1}
    * GET       /admin/subscribers?group=1  data=None                           200
    * POST      /admin/partner?group=1      data={'ID': '0', 'IP_ADDRESS': '127.0.0.1', 'PORT': '203', 'DESCRIPTION': 'Local Server'}
                                                                                201     {'PORT': 203}
    """

    @aiohttp_jinja2.template('admin_list.html')
    async def get(self) -> Dict:
        subscriber_access = create_subscriber_access(con=self.request.app['DISTRIBUTOR_DB'])
        data = await subscriber_access.get_subscribers()
        return {'data': data}


class PartnerAdmin(web.View):

    @aiohttp_jinja2.template('admin.html')
    async def get(self) -> Dict:
        value = self.request.match_info.get('value', '80')
        if ':' in value:
            netloc = value
        else:
            netloc = ':'.join([self.request.remote, value])
        partner_access = create_partner_access(con=self.request.app['DISTRIBUTOR_DB'])
        try:
            log.debug(netloc)
            data = await partner_access.get_partner_data(netloc=netloc)
        except IndexError:
            raise web.HTTPBadRequest()
        log.debug(data.to_dict())
        return data.to_dict()

    @aiohttp_jinja2.template('admin.html')
    async def post(self) -> Dict:
        multi_data = await self.request.post()
        log.debug(multi_data)
        partner_access = create_partner_access(con=self.request.app['DISTRIBUTOR_DB'])
        try:
            partner = await partner_access.change_partner_data(**multi_data)
            return partner.to_dict()
        except sqlite3.DatabaseError as error:
            log.error(error)
            raise web.HTTPException()


class Distributor(web.View):
    async def post(self) -> web.Response:
        name: str = self.request.match_info.get('name', "orders")
        event_access = create_event_access(self.request.app['DISTRIBUTOR_DB'])
        transaction = create_transaction(self.request.app['DISTRIBUTOR_DB'])
        try:
            event = await event_access.get_event_data(url=name, method=self.request.method)
        except IndexError:
            return web.HTTPNotImplemented()
        response: State = await transaction.receive(self.request, event)

        if response.code not in {200, 201}:
            log.warning(response.message)
            return web.Response(text=response.reason, status=response.code)
        response = await transaction.store()
        if response.code not in {200, 201}:
            log.warning(response.message)
            return web.Response(text=response.reason, status=response.code)
        return web.Response(text=transaction.message, status=201)

    async def put(self) -> web.Response:
        """
        Just call post()
        :return: response from post, see there
        """
        return await self.post()


async def start_background_tasks(_app):
    log.info(f"start distributor with version {__version__}")
    _app['LIKEMC_SPREAD'] = _app.loop.create_task(spread(_app), )


async def cleanup_background_tasks(_app):
    _app['LIKEMC_SPREAD'].cancel()
    await _app['LIKEMC_SPREAD']


async def spread(_app):
    while True:
        log.info(f"spread it at {datetime.now()}")
        try:
            task_access = create_task_access(_app['DISTRIBUTOR_DB'])
            await task_access.read_tasks(state=1)
            transaction = create_transaction(_app['DISTRIBUTOR_DB'])
            for data in task_access.get_data():
                code = await transaction.spread(data.id)
                log.info(code.message)
            processing_step = create_processing_step_access(_app['DISTRIBUTOR_DB'])
            for check_state in {1, 2}:  # the two check states are new and in progress
                await processing_step.read_processing_steps(state=check_state)
                async with aiohttp.ClientSession(loop=_app.loop, conn_timeout=1.0) as client:
                    for data in processing_step.get_data():
                        state = await transaction.process(data, client)
                        log.info(state)

            await asyncio.sleep(5)
        except asyncio.CancelledError:
            log.warning("Cancelled")
            return


@web.middleware
async def logger_middleware(request: web.Request, handler) -> web.Response:
    start_time = datetime.now()
    response = await handler(request)
    elapsed = datetime.now() - start_time
    log.info(request.method + ': ' + str(elapsed))
    return response


def apply_config(task_app: web.Application, config_file_name: str) -> int:
    config = ConfigParser()
    config.read(config_file_name)
    task_port: int = config.getint('SERVER', 'task')
    engine: sqlite3.Connection = prepare_connection(
        database_name=str(config.get('DATABASE', 'task_db')))
    task_app['DISTRIBUTOR_DB'] = engine
    return task_port


def apply_ssl(config_file_name: str) -> (ssl.SSLContext, None):
    config = ConfigParser()
    config.read(config_file_name)
    schema: str = config.get('SERVER', 'schema')
    if schema != 'https':
        return None
    certificate: str = config.get('SERVER', 'certificate')
    ssl_ctx: ssl.SSLContext = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_ctx.load_cert_chain(certfile=certificate, keyfile=certificate)
    return ssl_ctx


def apply_routes(task_app: web.Application) -> bool:
    task_app.router.add_route('*', '/transactions/{name}', Distributor)
    task_app.router.add_route('*', '/admin/partners/{value}', PartnerAdmin)
    task_app.router.add_route('*', '/admin/subscribers', SubscriberAdmin)
    task_app.router.add_static(path='./asynctransaction/static', prefix='/static')
    aiohttp_jinja2.setup(app=task_app, loader=FileSystemLoader('./asynctransaction/view'))
    return True


if __name__ == '__main__':
    app = web.Application()
    app.middlewares.append(logger_middleware)
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    apply_routes(app)
    port = apply_config(app, CONFIG_FILE_NAME)
    web.run_app(app=app, port=port, ssl_context=apply_ssl(CONFIG_FILE_NAME))
