import ast
import dataclasses
import json
import logging
from functools import partial
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

import aiohttp
import inflection
from caproto import ChannelData
from caproto.server import AsyncLibraryLayer, PVGroup, SubGroup, pvproperty

from .db_backed import (DatabaseBackedHelper, DatabaseHandler,
                        ManualElasticHandler)

_session = None
logger = logging.getLogger(__name__)


def get_global_session() -> aiohttp.ClientSession:
    """Get the shared aiohttp ClientSession."""
    global _session
    if _session is None:
        _session = aiohttp.ClientSession()
    return _session


def key_to_pv(key: str) -> str:
    """
    Take an archiver appliance JSON key and make a PV name out of it.

    Parameters
    ----------
    key : str
        The archiver appliance metrics key name.

    Example
    -------
    >>> key_to_pv("Avg time spent by getETLStreams() in ETL(0&raquo;1) (s/run)")
    'AvgTimeSpentByGetetlstreamsInEtl0To1SPerRun'
    """
    # Pre-filter: &raquo -> to
    key = key.replace("&raquo;", " to ")
    # Pre-filter: / -> per
    key = key.replace("/", " per ")
    # Pre-filter: ETL -> _ETL_
    key = key.replace("ETL", " ETL ")
    # Pre-filter: .*rate -> Rate
    if key.endswith('rate'):
        key = key[:-4] + 'Rate'

    # Parametrize it for consistency:
    parametrized = inflection.parameterize(key)
    return inflection.camelize(parametrized.replace("-", "_"))


def archiver_literal_eval(value: str) -> Any:
    """literaleval-like function for archiver metrics values."""
    try:
        evaluated = ast.literal_eval(value)
    except Exception:
        return value

    # Numbers such as: 160,732 become (160, 732)
    if isinstance(evaluated, tuple):
        return archiver_literal_eval(value.replace(',', ''))
    return evaluated


def _metric_value_to_kwargs(key: str, value: Any) -> dict:
    value = archiver_literal_eval(value)

    kwargs = {
        'doc': key,
        'record': {
            bool: 'bi',
            float: 'ai',
            int: 'longin',
            str: 'stringin',
        }[type(value)],
    }

    if isinstance(value, str):
        kwargs['report_as_string'] = True
        kwargs['max_length'] = 2000

    return {
        'value': value,
        'kwargs': kwargs,
    }


def instance_metrics_to_pvproperties(metrics_string: str) -> List[dict]:
    """
    Make a key-pvproperty kwarg dictionary from a metrics JSON string.

    The instance metrics are of the form (note the surrounding list):
        [{"instance1_key1": "value1", "instance1_key2": "value2"},
         {"instance2_key1": "value1", "instance2_key2": "value2"},
         ]
    """
    def to_instance_pv(instance_dict, key):
        return instance_dict['instance'] + ':' + key_to_pv(key)

    return [
        dict(
            name=to_instance_pv(instance_dict, key),
            **_metric_value_to_kwargs(key, value)
        )
        for instance_dict in json.loads(metrics_string)
        for key, value in instance_dict.items()
    ]


def detailed_metrics_to_pvproperties(instance: str, metrics_string: str) -> List[dict]:
    """
    Make a key-pvproperty kwarg dictionary from a metrics JSON string.

    These detailed metrics are a list of dictionaries with the keys: value,
    name, and source.
    """
    return [
        dict(
            name=key_to_pv(item['name']),
            **_metric_value_to_kwargs(item['name'], item["value"])
        )
        for item in json.loads(metrics_string)
    ]


@dataclasses.dataclass
class Request:
    """Dataclass representing an http request."""
    url: str
    parameters: Optional[dict] = None
    method: str = 'get'
    transformer: Optional[callable] = json.loads
    last_result: Optional[dict] = None
    last_raw_result: Optional[str] = None

    async def make(self, session: Optional[aiohttp.ClientSession] = None) -> dict:
        """
        Make a request and convert the JSON response to a dictionary.

        Parameters
        ----------
        session : aiohttp.ClientSession, optional
            The client session - defaults to using the globally shared one.
        """

        if session is None:
            session = get_global_session()

        method = {
            'get': session.get,
            'put': session.put,
        }[self.method]

        async with method(self.url, params=self.parameters) as response:
            data = await response.text()
            assert response.status == 200

        self.last_raw_result = data

        if self.transformer is not None:
            data = self.transformer(data)

        self.last_result = data
        return data


class JSONRequestGroup(PVGroup):
    """
    Generic request -> JSON response to PVGroup helper.
    """

    async def __ainit__(self):
        """
        A special async init handler.
        """

    async def update(self):
        ...

    @classmethod
    async def from_request(
            cls,
            name: str,
            request: Request,
            *,
            session: Optional[aiohttp.ClientSession] = None,
            ) -> Type['JSONRequestGroup']:
        """
        Make a request andn generate a new PVGroup based on the response.

        Parameters
        ----------
        name : str
            The new class name.

        request : Request (or sequence of Request)
            The request object (or objects) used to make the query and generate
            the PV database.

        session : aiohttp.ClientSession, optional
            The aiohttp client session (defaults to the global session).
        """
        if isinstance(request, Sequence):
            requests = list(request)
        else:
            requests = [request]

        clsdict = dict(
            requests=requests,
            key_to_attr_map={},
        )

        key_to_attr_map: Dict[str, str] = clsdict['key_to_attr_map']

        for request in requests:
            response = await request.make(session=session)
            for item in response:
                attr, prop = cls.create_pvproperty(item)
                if attr in clsdict:
                    logger.warning('Attribute shadowed: %s', attr)
                clsdict[attr] = prop
                key_to_attr_map[item['name']] = attr

        return type(name, (cls, ), clsdict)

    @classmethod
    def create_pvproperty(cls, item: Dict[str, Any]) -> Tuple[str, pvproperty]:
        """
        Create a pvproperty dynamically from a portion fo the JSON response.

        Parameters
        ----------
        item : dict
            Single dictionary of information from the response. Expected to
            contain the keys "name" and "value" at minimum. May also contain
            a pre-defined "attr".
        """
        # Shallow-copy the item and remove `attr`
        item = dict(item)
        attr = item.pop('attr', inflection.underscore(item['name']))
        attr = attr.replace(':', '_')
        if not attr.isidentifier():
            old_attr = attr
            attr = f'json_{abs(hash(attr))}'
            logger.warning('Invalid identifier: %s -> %s', old_attr, attr)

        kwargs = item.get('kwargs', {})
        return attr, pvproperty(name=item['name'], value=item['value'],
                                **kwargs)


class DatabaseBackedJSONRequestGroup(JSONRequestGroup):
    """
    Extends the JSON request-backed PVGroup by storing the gathered information
    in an external database instance.
    """
    handlers = {
        'elastic': ManualElasticHandler,
    }
    init_document: Optional[dict] = None

    db_helper = SubGroup(DatabaseBackedHelper)

    def __init__(self, *args,
                 index: Optional[str] = None,
                 backend: str,
                 url: str,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.init_document = None

        # Init here
        handler_class: Type[DatabaseHandler] = self.handlers[backend]
        self.db_helper.handler = handler_class(self, url, index=index)

    async def __ainit__(self):
        """
        A special async init handler.
        """
        await super().__ainit__()
        self.init_document = await self.db_helper.handler.get_last_document()


class Archstats(PVGroup):
    """
    EPICS Archiver Appliance statistics IOC.
    """

    updater = pvproperty(value=0, name='__UPDATER__', read_only=True)
    update_rate = 60

    def __init__(self, *args, appliance_url,
                 database_url='http://localhost:9200',
                 database_backend=None,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.appliance_url = appliance_url
        self.database_url = database_url
        self.database_backend = database_backend
        self._dynamic_groups = []
        self._document_count = {}

    async def __ainit__(self):
        """
        A special async init handler, finished prior to `caproto.server.run()`.
        """
        basic_metrics_req = Request(
            url=f'{self.appliance_url}mgmt/bpl/getApplianceMetrics',
            transformer=instance_metrics_to_pvproperties,

        )

        # await self._add_dynamic_group(
        #     'ApplianceMetricsGroup',
        #     basic_metrics_req,
        #     index='archiver_appliance_metrics',
        # )

        await basic_metrics_req.make()
        instances = [
            appliance_info['instance']
            for appliance_info in json.loads(basic_metrics_req.last_raw_result)
        ]
        for instance in instances:
            await self._add_dynamic_group(
                f'DetailedMetricsGroup{instance}',
                Request(
                    url=f'{self.appliance_url}mgmt/bpl/getApplianceMetricsForAppliance',
                    transformer=partial(detailed_metrics_to_pvproperties, instance),
                    parameters=dict(appliance=instance),
                ),
                index=f'archiver_appliance_metrics_{instance.lower()}',
                prefix=f'{instance}:',
            )

    async def _add_dynamic_group(
            self,
            class_name: str,
            request: Request,
            index: Optional[str] = None,
            prefix: str = '',
            ) -> DatabaseBackedJSONRequestGroup:
        """
        Add a dynamic PVGroup to be periodically updated.

        Parameters
        ----------
        class_name : str
            The class name for the new group.

        request : Request
            The request object used to make the query and generate the PV
            database.

        index : str, optional
            The index name to use.

        prefix : str, optional
            The prefix for the group, exclusive of ``self.prefix``.
        """

        group_cls = await DatabaseBackedJSONRequestGroup.from_request(
            class_name, request)
        group = group_cls(prefix=f'{self.prefix}{prefix}',
                          backend=self.database_backend,
                          url=self.database_url,
                          index=index, parent=self)

        self._dynamic_groups.append(group)
        self._document_count[group] = 0
        self._pvs_.update(group._pvs_)
        self.pvdb.update(group.pvdb)

        if hasattr(group, '__ainit__'):
            await group.__ainit__()

        return group_cls, group

    async def _update_group(self, group: DatabaseBackedJSONRequestGroup):
        """
        Update the dynamic group `group`.

        Parameters
        ----------
        group : DatabaseBackedJSONRequestGroup
            The group to update.
        """
        changed = False
        for request in group.requests:
            for item in await request.make():
                try:
                    attr = group.key_to_attr_map[item['name']]
                except KeyError:
                    self.log.warning('Saw new entry: %s', item)
                    continue

                prop = getattr(group, attr)
                try:
                    if prop.value != item['value']:
                        await prop.write(value=item['value'])  # , timestamp=timestamp)
                        changed = True
                except Exception:
                    self.log.exception('Failed to update %s to %s', prop, item)

        first_document = self._document_count[group] == 0
        if changed or (first_document and group.init_document is None):
            await group.db_helper.store()
            self._document_count[group] += 1

    @updater.startup
    async def updater(self, instance: ChannelData, async_lib: AsyncLibraryLayer):
        """
        Startup hook: periodically update the dynamic groups contained here.
        """
        while True:
            try:
                for group in self._dynamic_groups:
                    await self._update_group(group)
                    await async_lib.library.sleep(0.1)
            except Exception:
                self.log.exception('Update failed!')
                await async_lib.library.sleep(10.0)

            await async_lib.library.sleep(self.update_rate)
