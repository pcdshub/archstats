import abc
import datetime
import logging
import operator
# import typing
import uuid
from typing import Generator, Optional, Tuple

import caproto
import caproto.server
from caproto.server import (AsyncLibraryLayer, PVGroup, PvpropertyData,
                            SubGroup, pvproperty)
from elasticsearch import AsyncElasticsearch

logger = logging.getLogger(__name__)


class DatabaseHandler(abc.ABC):
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
    def handler(self) -> DatabaseHandler:
        """The database handler."""
        return self._db

    @handler.setter
    def handler(self, handler: DatabaseHandler):
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


class ElasticHandler(DatabaseHandler):
    """
    ElasticSearch-backed PVGroup.

    Assumptions:
    * Caproto PVGroup is the primary source of data; i.e., Data will not
      change in the database outside of caproto
    * Field information will not be stored
    """

    TIMESTAMP_KEY = 'timestamp'

    def __init__(self,
                 group: PVGroup,
                 db_skip_attributes: Optional[set] = None,
                 es: AsyncElasticsearch = None,
                 index: Optional[str] = None,
                 restore_on_startup: bool = True,
                 ):
        self.group = group

        default_idx = f'{group.name}-{group.prefix}'.replace(':', '_').lower()
        self.index = index or default_idx

        self.db_skip_attributes = db_skip_attributes or {}
        if es is None:
            es = AsyncElasticsearch(['localhost:9200'])
        self.es = es
        self.restore_on_startup = restore_on_startup
        self._restoring = False
        logger.warning('Elasticsearch: %s Index: %s', self.es, self.index)

    def new_id(self) -> str:
        """Generate a new document ID."""
        return str(uuid.uuid4())

    def _get_instances(self) -> Generator[Tuple[pvproperty, PvpropertyData], None, None]:
        """Get all pvproperty instances to save."""
        for dotted_attr, pvprop in self.group._pvs_.items():
            channeldata = operator.attrgetter(dotted_attr)(self.group)
            if '.' in dotted_attr:
                # one level deep for now
                ...
            elif dotted_attr not in self.db_skip_attributes:
                yield pvprop, channeldata

    def get_timestamp_from_instances(self, instances) -> datetime.datetime:
        """Determine the timestamp to use in the document."""
        latest_posix_stamp = max(
            channeldata.timestamp for _, channeldata in instances
        )

        return datetime.datetime.fromtimestamp(
            latest_posix_stamp
        )

    def create_document(self) -> dict:
        """Create a document based on the current IOC state."""
        instances = tuple(self._get_instances())
        if not instances:
            return {}

        document = {
            pvprop.attr_name: channeldata.value
            for pvprop, channeldata in instances
        }
        document[self.TIMESTAMP_KEY] = self.get_timestamp_from_instances(instances)
        return document

    async def get_last_document(self):
        """Get the latest document from the database."""
        result = await self.es.search(
            index=self.index,
            body={'sort': {self.TIMESTAMP_KEY: 'desc'}},
            size=1,
        )

        if result and result['hits']:
            try:
                return result['hits']['hits'][0]['_source']
            except (KeyError, IndexError):
                return None

    async def startup(self, group: PVGroup, async_lib: AsyncLibraryLayer):
        """Startup hook."""
        # 400 - ignore if index already exists
        await self.es.indices.create(index=self.index, ignore=400)

        if self.restore_on_startup:
            try:
                doc = await self.get_last_document()
            except Exception:
                self.group.log.exception('Failed to get the latest document')
            else:
                if doc is None:
                    self.group.log.warning(
                        'No document found to restore from.'
                    )
                    return

                try:
                    await self.restore_from_document(doc)
                except Exception:
                    self.group.log.exception(
                        'Failed to restore the latest document (%s)', doc
                    )

    async def restore_from_document(self, doc: dict):
        """Restore the PVGroup state from the given document."""
        timestamp = datetime.datetime.fromisoformat(doc[self.TIMESTAMP_KEY])
        timestamp: float = timestamp.timestamp()

        try:
            self._restoring = True
            for attr, value in doc.items():
                if attr in {self.TIMESTAMP_KEY, }:
                    continue

                try:
                    prop = getattr(self.group, attr)
                except AttributeError:
                    self.group.log.warning(
                        'Attribute no longer valid: %s (value=%s)', attr, value
                    )
                    continue

                try:
                    await prop.write(value=value, timestamp=timestamp)
                except Exception:
                    self.group.log.exception(
                        'Failed to restore %s value=%s', attr, value
                    )
                else:
                    self.group.log.info('Restored %s = %s', attr, value)
        finally:
            self._restoring = False

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
        await self.es.create(
            index=self.index,
            id=self.new_id(),
            body=self.create_document()
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


class Test(PVGroup):
    db_helper = SubGroup(DatabaseBackedHelper)
    item_a = pvproperty(value=5)
    item_b = pvproperty(value=6)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_helper.handler = ManualElasticHandler(self)

    async def group_write(self, instance: PvpropertyData, value):
        'Generic write called for channels without `put` defined'
        await self.db_helper.write(instance, value)
        return value

    @item_a.startup
    async def item_a(self, instance: PvpropertyData, async_lib: AsyncLibraryLayer):
        """
        Startup hook for item_a.
        """
        while True:
            await async_lib.library.sleep(1.0)
            await self.db_helper.store()


def main():
    ioc_options, run_options = caproto.server.ioc_arg_parser(
        default_prefix='ARCH:',
        desc='Test',
    )

    ioc = Test(**ioc_options)
    caproto.server.run(ioc.pvdb, **run_options)


if __name__ == '__main__':
    main()
