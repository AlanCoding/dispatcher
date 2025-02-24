import logging
import select
from typing import Any, AsyncGenerator, Callable, Coroutine, Generator, Iterator, Optional, Union
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from multiprocessing import Queue
import time

import psycopg

from dispatcher.utils import resolve_callable

logger = logging.getLogger(__name__)


"""This module exists under the theory that dispatcher messaging should be swappable

to different message busses eventually.
That means that the main code should never import psycopg.
Thus, all psycopg-lib-specific actions must happen here.
"""


async def acreate_connection(**config) -> psycopg.AsyncConnection:
    "Create a new asyncio connection"
    return await psycopg.AsyncConnection.connect(**config)


def create_connection(**config) -> psycopg.Connection:
    return psycopg.Connection.connect(**config)


def current_notifies(conn: psycopg.Connection) -> Generator[psycopg.connection.Notify, None, None]:
    """Altered version of .notifies method from psycopg library

    Taken from AWX, only used for synchronous listening, same notify-or-timeout problem
    This removes the outer while True loop so that we only process
    queued notifications
    """
    with conn.lock:
        try:
            ns = conn.wait(psycopg.generators.notifies(conn.pgconn))
        except psycopg.errors._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)
    for pgn in ns:
        if hasattr(conn.pgconn, '_encoding'):
            # later, like 3.2+ versions
            enc = conn.pgconn._encoding
        else:
            # For earlier versions, of course we have to ignore typing
            # because psycopg.Connection having _encodings is version dependent
            enc = psycopg._encodings.pgconn_encoding(conn.pgconn)  # type: ignore
        n = psycopg.connection.Notify(pgn.relname.decode(enc), pgn.extra.decode(enc), pgn.be_pid)
        yield n


class Broker:

    def __init__(
        self,
        config: Optional[dict] = None,
        async_connection_factory: Optional[str] = None,
        sync_connection_factory: Optional[str] = None,
        sync_connection: Optional[psycopg.Connection] = None,
        async_connection: Optional[psycopg.AsyncConnection] = None,
        channels: Union[tuple, list] = (),
        default_publish_channel: Optional[str] = None,
    ) -> None:
        """
        config - kwargs to psycopg connect classes, if creating connection this way
        (a)sync_connection_factory - importable path to callback for creating
          the psycopg connection object, the normal or synchronous version
          this will have the config passed as kwargs, if that is also given
        async_connection - directly pass the async connection object
        sync_connection - directly pass the async connection object
        channels - listening channels for the service and used for control-and-reply
        default_publish_channel - if not specified on task level or in the submission
          by default messages will be sent to this channel.
          this should be one of the listening channels for messages to be received.
        """
        if not (config or async_connection_factory or async_connection):
            raise RuntimeError('Must specify either config or async_connection_factory')

        if not (config or sync_connection_factory or sync_connection):
            raise RuntimeError('Must specify either config or sync_connection_factory')

        self._async_connection_factory = async_connection_factory
        self._async_connection = async_connection

        self._sync_connection_factory = sync_connection_factory
        self._sync_connection = sync_connection

        if config:
            self._config: dict = config.copy()
            self._config['autocommit'] = True
        else:
            self._config = {}

        self.channels = channels
        self.default_publish_channel = default_publish_channel

    def get_publish_channel(self, channel: Optional[str] = None) -> str:
        "Handle default for the publishing channel for calls to publish_message, shared sync and async"
        if channel is not None:
            return channel
        elif self.default_publish_channel is not None:
            return self.default_publish_channel
        elif len(self.channels) == 1:
            # de-facto default channel, because there is only 1
            return self.channels[0]

        raise ValueError('Could not determine a channel to use publish to from settings or PGNotify config')

    # --- asyncio connection methods ---

    async def aget_connection(self) -> psycopg.AsyncConnection:
        "Return existing connection or create a new one"
        if not self._async_connection:
            if self._async_connection_factory:
                factory = resolve_callable(self._async_connection_factory)
                if not factory:
                    raise RuntimeError(f'Could not import async connection factory {self._async_connection_factory}')
                connection = await factory(**self._config)
            elif self._config:
                connection = await acreate_connection(**self._config)
            else:
                raise RuntimeError('Could not construct async connection for lack of config or factory')
            self._async_connection = connection
            return connection  # slightly weird due to MyPY
        return self._async_connection

    def get_listen_query(self, channel: str) -> psycopg.sql.Composed:
        """Returns SQL command for listening on pg_notify channel

        This uses the psycopg utilities which ensure correct escaping so SQL injection is not possible.
        Return value is a valid argument for cursor.execute()
        """
        return psycopg.sql.SQL("LISTEN {};").format(psycopg.sql.Identifier(channel))

    async def aprocess_notify(
        self, connected_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None
    ) -> AsyncGenerator[tuple[str, str], None]:  # public
        connection = await self.aget_connection()
        async with connection.cursor() as cur:
            for channel in self.channels:
                await cur.execute(self.get_listen_query(channel))
                logger.info(f"Set up pg_notify listening on channel '{channel}'")

            if connected_callback:
                await connected_callback()

            while True:
                logger.debug('Starting listening for pg_notify notifications')
                async for notify in connection.notifies():
                    yield notify.channel, notify.payload

    async def apublish_message(self, channel: Optional[str] = None, message: str = '') -> None:  # public
        """asyncio way to publish a message, used to send control in control-and-reply

        Not strictly necessary for the service itself if it sends replies in the workers,
        but this may change in the future.
        """
        connection = await self.aget_connection()
        channel = self.get_publish_channel(channel)

        async with connection.cursor() as cur:
            if not message:
                await cur.execute(f'NOTIFY {channel};')
            else:
                await cur.execute(f"NOTIFY {channel}, '{message}';")

        logger.debug(f'Sent pg_notify message of {len(message)} chars to {channel}')

    async def aclose(self) -> None:
        if self._async_connection:
            await self._async_connection.close()
            self._async_connection = None

    # --- synchronous connection methods ---

    def get_connection(self) -> psycopg.Connection:
        if not self._sync_connection:
            if self._sync_connection_factory:
                factory = resolve_callable(self._sync_connection_factory)
                if not factory:
                    raise RuntimeError(f'Could not import connection factory {self._sync_connection_factory}')
                connection = factory(**self._config)
            elif self._config:
                connection = create_connection(**self._config)
            else:
                raise RuntimeError('Could not construct connection for lack of config or factory')
            self._sync_connection = connection
            return connection
        return self._sync_connection

    def process_notify_forever(self, queue: Queue, connected_callback: Optional[Callable] = None, max_messages: int = 1) -> None:
        "Blocking method that listens for messages on subscribed pg_notify channels forever or until max_messages received, writes to queue"
        connection = self.get_connection()

        with connection.cursor() as cur:
            for channel in self.channels:
                cur.execute(self.get_listen_query(channel))
                logger.info(f"Set up pg_notify listening on channel '{channel}'")

            if connected_callback:
                connected_callback()

            logger.debug('Starting listening for pg_notify notifications')
            msg_ct = 0
            while True:
                # select.select([connection], [], [])

                while connection.notifies():
                    notify = connection.notifies().pop(0)
                    queue.put((notify.channel, notify.payload))
                    msg_ct += 1

                if msg_ct >= max_messages:
                    break

    def process_notify(self, connected_callback: Optional[Callable] = None, timeout: float = 5.0, max_messages: int = 1) -> list[tuple[str, str]]:
        """Blocking method that listens for messages on subscribed pg_notify channels forever

        This has two different exit conditions:
        - received max_messages number of messages or more
        - taken longer than the specified timeout condition
        """
        queue = Queue()
        messages = []

        with ThreadPoolExecutor(max_workers=2) as pool:
            listen_fut = pool.submit(self.process_notify_forever, queue, connected_callback, max_messages)
            sleep_fut = pool.submit(lambda: time.sleep(timeout))

            _, to_cancel = wait([listen_fut, sleep_fut], return_when=FIRST_COMPLETED)

            print('canceling threads')
            for fut in to_cancel:
                fut.cancel()

            while not queue.empty():
                messages.append(queue.get())
        return messages


    def publish_message(self, channel: Optional[str] = None, message: str = '') -> None:
        connection = self.get_connection()
        channel = self.get_publish_channel(channel)

        with connection.cursor() as cur:
            if message:
                cur.execute('SELECT pg_notify(%s, %s);', (channel, message))
            else:
                cur.execute(f'NOTIFY {channel};')

        logger.debug(f'Sent pg_notify message of {len(message)} chars to {channel}')

    def close(self) -> None:
        if self._sync_connection:
            self._sync_connection.close()
            self._sync_connection = None


class ConnectionSaver:
    def __init__(self) -> None:
        self._connection: Optional[psycopg.Connection] = None
        self._async_connection: Optional[psycopg.AsyncConnection] = None


connection_save = ConnectionSaver()


def connection_saver(**config) -> psycopg.Connection:
    """
    This mimics the behavior of Django for tests and demos
    Philosophically, this is used by an application that uses an ORM,
    or otherwise has its own connection management logic.
    Dispatcher does not manage connections, so this a simulation of that.
    """
    if connection_save._connection is None:
        config['autocommit'] = True
        connection_save._connection = create_connection(**config)
    return connection_save._connection


async def async_connection_saver(**config) -> psycopg.AsyncConnection:
    """
    This mimics the behavior of Django for tests and demos
    Philosophically, this is used by an application that uses an ORM,
    or otherwise has its own connection management logic.
    Dispatcher does not manage connections, so this a simulation of that.
    """
    if connection_save._async_connection is None:
        config['autocommit'] = True
        connection_save._async_connection = await acreate_connection(**config)
    return connection_save._async_connection
