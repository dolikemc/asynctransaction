import logging

from aiohttp import web
from asynctransaction.server.distributor import logger_middleware


class Client(web.View):
    async def post(self) -> web.Response:
        log.info(self.request.method)
        return web.HTTPOk()

    async def put(self) -> web.Response:
        return await self.post()


if __name__ == '__main__':
    log = logging.getLogger('asynctransaction.server.client')
    app = web.Application()
    app.middlewares.append(logger_middleware)
    app.router.add_route('*', '/transactions/{name}', Client)
    web.run_app(app=app, port=3030)
