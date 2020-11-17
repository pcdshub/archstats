import asyncio
import os
import textwrap
import urllib

import caproto.server

from .archstats import Archstats


def get_archiver_url():
    archiver_url = os.environ.get(
        'ARCHIVER_URL', 'http://pscaa02.slac.stanford.edu:17665/'
    )
    if not archiver_url:
        raise RuntimeError('ARCHIVER_URL not set')

    if not archiver_url.startswith('http'):
        archiver_url = f'http://{archiver_url}'

    result: urllib.SplitResult = urllib.parse.urlsplit(archiver_url)
    return f'{result.scheme}://{result.netloc}/'


def main():
    ioc_options, run_options = caproto.server.ioc_arg_parser(
        default_prefix='ARCH:',
        desc=textwrap.dedent(Archstats.__doc__),
        supported_async_libs=('asyncio', ),
    )

    ioc = Archstats(appliance_url=get_archiver_url(), **ioc_options)
    if hasattr(ioc, '__ainit__'):
        loop = asyncio.get_event_loop()
        ainit_task = loop.create_task(ioc.__ainit__())
        loop.run_until_complete(ainit_task)
    caproto.server.run(ioc.pvdb, **run_options)


if __name__ == '__main__':
    main()
