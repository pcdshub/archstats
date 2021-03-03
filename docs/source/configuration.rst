Installation
------------

Install archstats in conda by performing the following::

    $ conda create -n archstats python=3.7
    $ conda activate archstats
    $ cd archstats
    $ pip install .


Configuration
-------------

.. list-table:: Environment Variables
    :header-rows: 1

    * - Environment variable
      - Default
      - Description

    * - ARCHIVER_URL
      - http://pscaa02.slac.stanford.edu:17665/
      - The management port URL. The default is SLAC-specific (sorry!)

    * - ARCHSTATS_DATABASE
      - elastic
      - The database type to use. Currently only elastic is supported.

    * - ARCHSTATS_DATABASE_URL
      - http://localhost:9200/
      - The database URL. The default assumes elasticsearch runs on this
        machine.

    * - ARCHSTATS_INDEX_FORMAT
      - archiver-appliance-stats
      - The default format for the Elasticsearch index.  May use variable
        `{appliance}` in name, such as:
        "archiver_appliance_statistics-{appliance}"

    * - ARCHSTATS_INDEX_SUFFIX
      -
      - The default suffix for the Elasticsearch index.  May use
        `strftime`-style strings to get monthly indices: `"-%Y.%m"`


With the above environment variables set appropriately, starting ``archstats``
should be as simple as running ``archstats``::

    $ archstats --list-pvs
    Elasticsearch: <AsyncElasticsearch([{'host': 'localhost', 'port': 9200}])> Index: archiver_appliance_metrics_pscaa01
    Elasticsearch: <AsyncElasticsearch([{'host': 'localhost', 'port': 9200}])> Index: archiver_appliance_metrics_pscaa02
    [I 14:26:23.806       server:  133] Asyncio server starting up...
    [I 14:26:23.806       server:  146] Listening on 0.0.0.0:5064
    [I 14:26:23.815       server:  205] Server startup complete.
    [I 14:26:23.815       server:  207] PVs available:
        ARCH:pscaa01:ApplianceIdentity
        ARCH:pscaa01:TotalPvCount
        ARCH:pscaa01:DisconnectedPvCount
        ARCH:pscaa01:ConnectedPvCount
        ARCH:pscaa01:PausedPvCount
        ARCH:pscaa01:TotalChannels
        ...

Information regarding the elasticsearch index will first be emitted.
``archstats`` will communicate with the specified archiver management interface
and dynamically create caproto `PVGroup` instances for each appliance.

You can then move on to viewing the data in grafana, or querying it by way of
EPICS::

    $ caget ARCH:pscaa02:SystemLoad ARCH:pscaa02:EngineHeap ARCH:pscaa02:EtlHeap ARCH:pscaa02:RetrievalHeapget ARCH:pscaa02:SystemLoad ARCH:pscaa02:EngineHeap ARCH:pscaa02:EtlHeap ARCH:pscaa02:RetrievalHea
    ARCH:pscaa02:SystemLoad        1.67
    ARCH:pscaa02:EngineHeap        48.1998
    ARCH:pscaa02:EtlHeap           23.9628
    ARCH:pscaa02:RetrievalHeap     21.7903
