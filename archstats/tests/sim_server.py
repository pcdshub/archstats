import json
import logging
import pathlib
from functools import partial

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

MODULE_PATH = pathlib.Path(__file__).parent
JSON_PATH = MODULE_PATH / 'json'
NUM_REQUESTS = 0


def increment_request_count():
    """Return and increment the number of requests."""
    global NUM_REQUESTS
    count = NUM_REQUESTS
    NUM_REQUESTS += 1
    return count


async def handle_data(request: aiohttp.web.BaseRequest):
    last_part = str(request.url).split('/')[-1]
    json_filename = JSON_PATH / last_part
    with open(f'{json_filename}.json', 'rt') as f:
        return web.Response(text=f.read())


async def handle_appliance_data(json_filename, request: aiohttp.web.BaseRequest,
                                increment: bool = False):
    json_filename = JSON_PATH / json_filename.format(appliance=request.query['appliance'])
    with open(f'{json_filename}.json', 'rt') as f:
        doc = json.load(f)

    if increment:
        # Increase the PV count on each request to make this a bit more
        # interesting:
        doc[1]['value'] = str(int(doc[1]['value']) + increment_request_count())
    return web.Response(text=json.dumps(doc))


app = web.Application()
app.add_routes(
    [
        web.get('/mgmt/bpl/getApplianceMetrics', handle_data),
        web.get('/mgmt/bpl/getStorageMetrics', handle_data),
        web.get('/mgmt/bpl/getInstanceMetrics', handle_data),
        web.get('/mgmt/bpl/getApplianceMetricsForAppliance',
                partial(handle_appliance_data,
                        'getApplianceMetricsForAppliance-{appliance}',
                        increment=True)
                ),
        web.get('/mgmt/bpl/getProcessMetricsDataForAppliance',
                partial(handle_appliance_data,
                        'getProcessMetricsDataForAppliance-{appliance}',
                        increment=False)
                ),
        web.get('/mgmt/bpl/getStorageMetricsForAppliance',
                partial(handle_appliance_data,
                        'getStorageMetricsForAppliance-{appliance}',
                        increment=False)
                ),
    ]
)


def main():
    logging.basicConfig()
    web.run_app(app)


if __name__ == '__main__':
    main()
