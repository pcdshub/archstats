===============================
archstats
===============================

.. image:: https://img.shields.io/travis/pcdshub/archstats.svg
        :target: https://travis-ci.org/pcdshub/archstats

.. image:: https://img.shields.io/pypi/v/archstats.svg
        :target: https://pypi.python.org/pypi/archstats


EPICS Archiver Appliance statistics IOC and Grafana dashboards

.. image:: https://raw.githubusercontent.com/pcdshub/archstats/assets/images/grafana_1.png
   :target: https://github.com/pcdshub/archstats/blob/master/grafana/example.json

Documentation
-------------

https://pcdshub.github.io/archstats/

Requirements
------------

* Python 3.7+
* aiohttp
* elasticsearch
* inflection

Installation
------------

..

    pip install git+https://github.com/pcdshub/archstats


Running the Tests
-----------------
::

  $ pip install -r dev-requirements.txt
  $ pytest -vv
