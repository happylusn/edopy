import os, logging
from aiohttp import web
from core.orm2 import create_pool, table, Model, IntegerField, StringField
from core.coroweb import add_routes, add_static
from config import configs
from core.function import init_jinja2

logging.basicConfig(level=logging.INFO)


@web.middleware
async def middleware1(request, handler):
    try:
        response = await handler(request)
        if isinstance(response, web.StreamResponse):
            return response
        return web.Response(text=str(response))
    except Exception as e:
        return web.Response(text=str(e))

app = web.Application(middlewares=[middleware1])

init_jinja2(app)

add_routes(app, os.path.join(os.path.dirname(__file__), './api/v1'), 'api.v1')

add_static(app)

app.cleanup_ctx.append(create_pool(**configs.db))

if __name__ == '__main__':
    web.run_app(app)

