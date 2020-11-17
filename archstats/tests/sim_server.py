import logging
import pathlib
from functools import partial

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

MODULE_PATH = pathlib.Path(__file__).parent
JSON_PATH = MODULE_PATH / 'json'


async def handle_data(request: aiohttp.web.BaseRequest):
    last_part = str(request.url).split('/')[-1]
    json_filename = JSON_PATH / last_part
    with open(f'{json_filename}.json', 'rt') as f:
        return web.Response(text=f.read())


async def handle_appliance_data(json_filename, request: aiohttp.web.BaseRequest):
    json_filename = JSON_PATH / json_filename.format(appliance=request.query['appliance'])
    with open(f'{json_filename}.json', 'rt') as f:
        return web.Response(text=f.read())


app = web.Application()
app.add_routes(
    [
        web.get('/mgmt/bpl/getApplianceMetrics', handle_data),
        web.get('/mgmt/bpl/getStorageMetrics', handle_data),
        web.get('/mgmt/bpl/getInstanceMetrics', handle_data),
        web.get('/mgmt/bpl/getApplianceMetricsForAppliance',
                partial(handle_appliance_data,
                        'getApplianceMetricsForAppliance-{appliance}')),
    ]
)


def main():
    web.run_app(app)


if __name__ == '__main__':
    main()
