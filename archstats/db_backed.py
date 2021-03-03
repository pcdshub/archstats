import abc
import dataclasses
import datetime
import json
import logging
import math
import operator
import uuid
from typing import Any, Dict, Generator, Optional, Sequence, Tuple, Type

import aiohttp
import inflection
from caproto.server import (AsyncLibraryLayer, PVGroup, PvpropertyData,
                            SubGroup, pvproperty)
from elasticsearch import AsyncElasticsearch

logger = logging.getLogger(__name__)
_session = None


class DatabaseHandlerInterface(abc.ABC):
    """Database handler interface class."""
    # @abc.abstractmethod
    # def set_group(self, group: PVGroup):
    #     ...

    @abc.abstractmethod
    async def startup(self, group: PVGroup, async_lib: AsyncLibraryLayer):
        """Startup hook."""
        ...

    @abc.abstractmethod
    async def shutdown(self, group: PVGroup, async_lib: AsyncLibraryLayer):
        """Shutdown hook."""
        ...

    async def get_last_document(self) -> dict:
        """Get the last document from the database."""

    @abc.abstractmethod
    async def write(self, instance: PvpropertyData, value):
        """
        Write, or queue writing, a new single value into the database.

        Parameters
        ----------
        instance : PvpropertyData

        value : any
            The value to write.
        """
        ...


class DatabaseBackedHelper(PVGroup):
    """
    A helper SubGroup for synchronizing a database with a PVGroups' values.
    """
    # TODO: I wanted this to be a mixin, but caproto server support failed me
    db_helper = pvproperty(name='__db_helper__', value=0)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db = None

    @property
    def handler(self) -> DatabaseHandlerInterface:
        """The database handler."""
        return self._db

    @handler.setter
    def handler(self, handler: DatabaseHandlerInterface):
        if self._db is not None:
            raise RuntimeError('Cannot set `db` twice.')

        self._db = handler

    async def write(self, instance: PvpropertyData, value):
        """Call on all writes."""
        await self._db.write(instance, value)

    @db_helper.startup
    async def db_helper(self, instance: PvpropertyData, async_lib: AsyncLibraryLayer):
        """
        Startup hook for db_helper.
        """
        await self._db.startup(self, async_lib)

    @db_helper.shutdown
    async def db_helper(self, instance: PvpropertyData, async_lib: AsyncLibraryLayer):
        """
        Shutdown hook for db_helper.
        """
        await self._db.shutdown(self, async_lib)

    async def store(self):
        """Store all data as a new document."""
        await self._db.store()


def get_latest_timestamp(instances: Tuple[PvpropertyData, ...]) -> datetime.datetime:
    """Determine the latest timestamp for use in a document."""
    latest_posix_stamp = max(
        channeldata.timestamp for channeldata in instances
    )

    return datetime.datetime.fromtimestamp(latest_posix_stamp).astimezone()


async def restore_from_document(group: PVGroup, doc: dict,
                                timestamp_key: str = '@timestamp'):
    """Restore the PVGroup state from the given document."""
    timestamp = doc[timestamp_key]
    if isinstance(timestamp, str):
        timestamp = datetime.datetime.fromisoformat(timestamp).timestamp()

    for attr, value in doc.items():
        if attr == timestamp_key:
            continue

        try:
            prop = getattr(group, attr)
        except AttributeError:
            group.log.warning(
                'Attribute no longer valid: %s (value=%s)', attr, value
            )
            continue

        try:
            await prop.write(value=value, timestamp=timestamp)
        except Exception:
            group.log.exception(
                'Failed to restore %s value=%s', attr, value
            )
        else:
            group.log.info('Restored %s = %s', attr, value)


class DatabaseHandler(DatabaseHandlerInterface):
    TIMESTAMP_KEY: str = '@timestamp'
    NAN_VALUE = math.nan

    def get_instances(self) -> Generator[PvpropertyData, None, None]:
        """Get all pvproperty instances to save."""
        for dotted_attr, pvprop in self.group._pvs_.items():
            channeldata = operator.attrgetter(dotted_attr)(self.group)
            if '.' in dotted_attr:
                # one level deep for now
                ...
            elif dotted_attr not in self.skip_attributes:
                yield channeldata

    def get_timestamp_from_instances(
            self, instances: Tuple[PvpropertyData, ...]
            ) -> datetime.datetime:
        """Get the timestamp to use in the document, given the instances."""
        # By default, get the latest timestamp:
        return get_latest_timestamp(instances)

    def replace_nan(self, value):
        """
        Some databases may be unable to store NaN values.  Replace them
        on a class-by-class basis with ``NAN_VALUE``.

        Parameters
        ----------
        value : any
            The value to check.

        Returns
        -------
        value : any
            NAN_VALUE if the input is NaN, else the original value.
        """
        try:
            if math.isnan(value):
                return self.NAN_VALUE
        except TypeError:
            ...
        return value

    def create_document(self) -> Optional[dict]:
        """Create a document based on the current IOC state."""
        instances = tuple(self.get_instances())
        document = {
            channeldata.pvspec.attr: self.replace_nan(channeldata.value)
            for channeldata in instances
        }

        if not document:
            return None

        # Keep the document timestamp in UTC time:
        dt = self.get_timestamp_from_instances(instances)
        document[self.TIMESTAMP_KEY] = dt.astimezone(datetime.timezone.utc)
        return document

    async def restore_from_document(self, doc: dict):
        """Restore the PVGroup state from the given document."""
        try:
            self._restoring = True
            await restore_from_document(
                group=self.group, doc=doc, timestamp_key=self.TIMESTAMP_KEY
            )
        finally:
            self._restoring = False


class ElasticHandler(DatabaseHandler):
    """
    ElasticSearch-backed PVGroup.

    Assumptions:
    * Caproto PVGroup is the primary source of data; i.e., Data will not
      change in the database outside of caproto
    * Field information is not currently stored
    """

    index: str
    group: PVGroup
    es: AsyncElasticsearch
    restore_on_startup: bool
    _dated_index: str
    _restoring: bool
    date_suffix_format = '-%Y.%m.%d'
    NAN_VALUE: float = 0.0   # sorry :(

    def __init__(self,
                 group: PVGroup,
                 url: str,
                 skip_attributes: Optional[set] = None,
                 es: AsyncElasticsearch = None,
                 index: Optional[str] = None,
                 index_suffix: Optional[str] = None,
                 restore_on_startup: bool = True,
                 ):
        self.group = group

        default_idx = inflection.underscore(
            f'{group.name}-{group.prefix}'.replace(':', '_')
        ).lower()

        self.index = index or default_idx
        self.index_suffix = index_suffix or ''
        self.index_search_glob = f'{self.index}*',
        self.group.log.info(
            '%s using elastic index %r (suffix %r)',
            group, self.index, self.index_suffix
        )
        self._dated_index = None
        self.get_last_document_query = {
           'sort': {self.TIMESTAMP_KEY: 'desc'}
        }

        self.skip_attributes = skip_attributes or {}
        if es is None:
            es = AsyncElasticsearch([url])
        self.es = es
        self.restore_on_startup = restore_on_startup
        self._restoring = False
        logger.warning(
            'Elasticsearch: %s Index: %r %s',
            self.es, self.index,
            f'(suffix {self.index_suffix!r})' if self.index_suffix else ''
        )

    def new_id(self) -> str:
        """Generate a new document ID."""
        return str(uuid.uuid4())

    async def get_last_document(self) -> dict:
        """Get the latest document from the database."""
        result = await self.es.search(
            index=self.index_search_glob,
            body=self.get_last_document_query,
            size=1,
        )

        if result and result['hits']:
            try:
                return result['hits']['hits'][0]['_source']
            except (KeyError, IndexError):
                return None

    async def get_dated_index_name(self) -> str:
        """Index name with a date suffix ({index}-{date_suffix})."""
        index = f'{self.index}{self.formatted_index_suffix}'
        if index != self._dated_index:
            self._dated_index = index
            # 400 - ignore if index already exists
            await self.es.indices.create(index=index, ignore=400)

        return index

    @property
    def formatted_index_suffix(self) -> str:
        """Optionally a UTC time date suffix for use with the index name."""
        return datetime.datetime.utcnow().strftime(self.index_suffix)

    async def startup(self, group: PVGroup, async_lib: AsyncLibraryLayer):
        """Startup hook."""
        if not self.restore_on_startup:
            return

        try:
            doc = await self.get_last_document()
        except Exception:
            self.group.log.exception('Failed to get the latest document')
            return

        if doc is None:
            self.group.log.warning('No document found to restore from.')
            return

        try:
            await self.restore_from_document(doc)
        except Exception:
            self.group.log.exception(
                'Failed to restore the latest document (%s)', doc
            )
        else:
            self.group.log.info('Restored state from last document')
            self.group.log.debug('Restored state from last document: %s', doc)

    async def shutdown(self, group: PVGroup, async_lib: AsyncLibraryLayer):
        """Shutdown hook."""
        ...

    async def write(self, instance: PvpropertyData, value):
        """
        Write a new single value into the database.

        Parameters
        ----------
        instance : PvpropertyData

        value : any
            The value to write.
        """
        if not self._restoring:
            ...

    async def store(self):
        """Store all data as a new document."""
        index = await self.get_dated_index_name()
        document = self.create_document()
        await self.es.create(
            index=index,
            id=self.new_id(),
            body=document,
        )


class ManualElasticHandler(ElasticHandler):
    """
    ElasticSearch-backed PVGroup.

    The PVGroup must call `store` at what it deems the "correct" time to store
    an update.
    """


class AutomaticElasticHandler(ElasticHandler):
    """
    ElasticSearch-backed PVGroup.

    Tracks changes, obeying `min_write_period` in order to automatically
    populate the database with new documents.

    (TODO)
    """
    def __init__(self,
                 *args,
                 min_write_period: float = 1.0,
                 **kwargs
                 ):
        super().__init__(*args, **kwargs)
        self.min_write_period = min_write_period


@dataclasses.dataclass
class Response:
    timestamp: datetime.datetime
    raw: str
    data: dict

    def get_time_since(self) -> datetime.timedelta:
        """Time since the request was made."""
        return datetime.datetime.now() - self.timestamp


@dataclasses.dataclass
class Request:
    """Dataclass representing an http request."""
    url: str
    parameters: Optional[dict] = None
    method: str = 'get'
    transformer: Optional[callable] = json.loads

    last_response: Optional[Response] = None
    cache_period: Optional[float] = 2.0

    async def make(self, session: Optional[aiohttp.ClientSession] = None) -> dict:
        """
        Make a request and convert the JSON response to a dictionary.

        Parameters
        ----------
        session : aiohttp.ClientSession, optional
            The client session - defaults to using the globally shared one.
        """

        if self.cache_period and self.last_response is not None:
            if self.last_response.get_time_since().total_seconds() < self.cache_period:
                logger.debug('Using cached response for %s (%s)', self.url,
                             self.parameters)
                return self.last_response.data

        if session is None:
            session = get_global_session()

        method = {
            'get': session.get,
            'put': session.put,
        }[self.method]

        async with method(self.url, params=self.parameters) as response:
            raw_response = await response.text()
            assert response.status == 200

        if self.transformer is None:
            data = raw_response
        else:
            data = self.transformer(raw_response)

        self.last_response = Response(
            timestamp=datetime.datetime.now(),
            raw=raw_response,
            data=data,
        )
        return data


def get_global_session() -> aiohttp.ClientSession:
    """Get the shared aiohttp ClientSession."""
    global _session
    if _session is None:
        _session = aiohttp.ClientSession()
    return _session


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
                 index_suffix: Optional[str] = None,
                 backend: str,
                 url: str,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.init_document = None

        # Init here
        handler_class: Type[DatabaseHandler] = self.handlers[backend]
        self.db_helper.handler = handler_class(
            self, url, index=index, index_suffix=index_suffix
        )

    async def __ainit__(self):
        """
        A special async init handler.
        """
        await super().__ainit__()
        try:
            self.init_document = await self.db_helper.handler.get_last_document()
        except Exception:
            logger.warning(
                'Unable to get last database document; are we starting from '
                'scratch or is there a misconfiguration?'
            )
            self.init_document = None
