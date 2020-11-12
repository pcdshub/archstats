import textwrap

import caproto.server

from .archstats import Archstats


def main():
    ioc_options, run_options = caproto.server.ioc_arg_parser(
        default_prefix='ARCH:',
        desc=textwrap.dedent(Archstats.__doc__)
    )

    ioc = Archstats(**ioc_options)
    caproto.server.run(ioc.pvdb, **run_options)


if __name__ == '__main__':
    main()
