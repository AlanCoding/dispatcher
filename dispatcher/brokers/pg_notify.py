import logging

import psycopg

from dispatcher.brokers.base import BaseBroker
from dispatcher.utils import resolve_callable

logger = logging.getLogger(__name__)


"""This module exists under the theory that dispatcher messaging should be swappable

to different message busses eventually.
That means that the main code should never import psycopg.
Thus, all psycopg-lib-specific actions must happen here.
"""


class PGNotifyBase(BaseBroker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self._config:
            self._config = self._config.copy()
            self._config['autocommit'] = True


class AsyncBroker(PGNotifyBase):
    async def get_connection(self) -> psycopg.AsyncConnection:
        if not self._connection:
            if self._async_connection_factory:
                factory = resolve_callable(self._async_connection_factory)
                if self._config:
                    self._connection = await factory(**self._config)
                else:
                    self._connection = await factory()
            elif self._config:
                self._connection = await AsyncBroker.create_connection(**self._config)
            else:
                raise RuntimeError('Could not construct async connection for lack of config or factory')
        return self._connection

    @staticmethod
    async def create_connection(config) -> psycopg.AsyncConnection:
        return await psycopg.AsyncConnection.connect(**config)

    async def aprocess_notify(self, connected_callback=None):
        connection = await self.get_connection()
        async with connection.cursor() as cur:
            for channel in self.channels:
                await cur.execute(f"LISTEN {channel};")
                logger.info(f"Set up pg_notify listening on channel '{channel}'")

            if connected_callback:
                await connected_callback()

            while True:
                logger.debug('Starting listening for pg_notify notifications')
                async for notify in connection.notifies():
                    yield notify.channel, notify.payload

    async def apublish_message(self, channel, payload=None) -> None:
        connection = await self.get_connection()
        async with connection.cursor() as cur:
            if not payload:
                await cur.execute(f'NOTIFY {channel};')
            else:
                await cur.execute(f"NOTIFY {channel}, '{payload}';")

    async def aclose(self) -> None:
        if self._connection:
            await self._connection.close()
            self._connection = None


connection_save = object()


def connection_saver(**config):
    """
    This mimics the behavior of Django for tests and demos
    Philosophically, this is used by an application that uses an ORM,
    or otherwise has its own connection management logic.
    Dispatcher does not manage connections, so this a simulation of that.
    """
    if not hasattr(connection_save, '_connection'):
        config['autocommit'] = True
        connection_save._connection = SyncBroker.connect(**config)
    return connection_save._connection


class SyncBroker(PGNotifyBase):
    def get_connection(self) -> psycopg.Connection:
        if not self._connection:
            if self._sync_connection_factory:
                factory = resolve_callable(self._sync_connection_factory)
                if not factory:
                    raise RuntimeError(f'Could not import connection factory {self._sync_connection_factory}')
                if self._config:
                    self._connection = factory(**self._config)
                else:
                    self._connection = factory()
            elif self._config:
                self._connection = SyncBroker.create_connection(**self._config)
            else:
                raise RuntimeError('Cound not construct synchronous connection for lack of config or factory')
        return self._connection

    @staticmethod
    def create_connection(config) -> psycopg.Connection:
        return psycopg.Connection.connect(**config)

    def publish_message(self, queue, message):
        connection = self.get_connection()

        with connection.cursor() as cur:
            cur.execute('SELECT pg_notify(%s, %s);', (queue, message))

        logger.debug(f'Sent pg_notify message to {queue}')

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
