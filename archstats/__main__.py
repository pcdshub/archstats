import asyncio
import os
import textwrap
import urllib
from typing import Tuple

import caproto.server

from .archstats import Archstats

SUPPORTED_DATABASE_BACKENDS = {'elastic', }


def get_archiver_url() -> str:
    """Get the archiver appliance interface URL from the environment."""
    archiver_url = os.environ.get(
        'ARCHIVER_URL', 'http://pscaa02.slac.stanford.edu:17665/'
    )
    if not archiver_url:
        raise RuntimeError('ARCHIVER_URL not set')

    if not archiver_url.startswith('http'):
        archiver_url = f'http://{archiver_url}'

    result: urllib.SplitResult = urllib.parse.urlsplit(archiver_url)
    return f'{result.scheme}://{result.netloc}/'


def get_database() -> Tuple[str, str]:
    """Get the database backend and URL from the environment."""
    backend = os.environ.get('ARCHSTATS_DATABASE', 'elastic')
    url = os.environ.get('ARCHSTATS_DATABASE_URL', 'http://localhost:9200/')
    if not url:
        raise RuntimeError('ARCHSTATS_DATABASE_URL unset')
    if not backend:
        raise RuntimeError('ARCHSTATS_DATABASE unset')
    if backend not in SUPPORTED_DATABASE_BACKENDS:
        raise RuntimeError(f'Unsupported database backend: {backend}')

    return backend, url


def main():
    """Run archstats based on command-line arguments."""
    ioc_options, run_options = caproto.server.ioc_arg_parser(
        default_prefix='ARCH:',
        desc=textwrap.dedent(Archstats.__doc__),
        supported_async_libs=('asyncio', ),
    )

    database, database_url = get_database()

    ioc = Archstats(
        appliance_url=get_archiver_url(),
        database_backend=database,
        database_url=database_url,
        **ioc_options
    )

    if hasattr(ioc, '__ainit__'):
        loop = asyncio.get_event_loop()
        ainit_task = loop.create_task(ioc.__ainit__())
        loop.run_until_complete(ainit_task)

    caproto.server.run(ioc.pvdb, **run_options)
    return ioc


if __name__ == '__main__':
    main()
