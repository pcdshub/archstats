Grafana
-------

In addition to providing archiver status by way of EPICS PVs, archstats
optionally can write to an elasticsearch database for long-term statistics.
This type of document database is natively supported by Kibana and Grafana.

An example Grafana dashboard that looks like the following is provided in this
repository:

.. image:: https://raw.githubusercontent.com/pcdshub/archstats/assets/images/grafana_1.png
   :target: https://github.com/pcdshub/archstats/blob/master/grafana/example.json

.. image:: https://raw.githubusercontent.com/pcdshub/archstats/assets/images/grafana_2.png
   :target: https://github.com/pcdshub/archstats/blob/master/grafana/example.json

It will need to be tailored for your site, as your elasticsearch index names
will depend on your archiver appliance setup.

Elasticsearch Index
^^^^^^^^^^^^^^^^^^^

The index name will be in the following format::

    [archiver_appliance_metrics_APPLIANCE_NAME_HERE]-YYYY.MM.DD

Replacing ``APPLIANCE_NAME_HERE`` with your appliance name.  The YYYY.MM.DD
portion at the end indicates that the index will be updated on a daily basis.
Elasticsearch handles these multiple indices natively.

With the above, the datasource configuration should look like the following in
Grafana:

.. image:: https://raw.githubusercontent.com/pcdshub/archstats/assets/images/datasource.png
