from django.core import signals
from django.db.utils import (
    DEFAULT_DB_ALIAS,
    DJANGO_VERSION_PICKLE_KEY,
    ConnectionHandler,
    ConnectionRouter,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from django.utils.connection import ConnectionProxy, _async_connection

__all__ = [
    "close_old_connections",
    "connection",
    "connections",
    "router",
    "DatabaseError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "DataError",
    "NotSupportedError",
    "Error",
    "InterfaceError",
    "OperationalError",
    "DEFAULT_DB_ALIAS",
    "DJANGO_VERSION_PICKLE_KEY",
]

connections = ConnectionHandler()


class new_connection:
    def __init__(self, using=DEFAULT_DB_ALIAS):
        self.using = using

    async def __aenter__(self):
        self.force_rollback = False
        if connections._in_test is True:
            try:
                _async_connection.get()
            except LookupError:
                # this is the first conneciton, ie: the outermost one.
                self.force_rollback = True

        self.conn = connections.create_connection(self.using)
        self.token = _async_connection.set(self.conn)
        if self.force_rollback is True:
            await self.conn.aset_autocommit(False)

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        autocommit = await self.conn.aget_autocommit()
        if autocommit is False:
            if exc_type is None and self.force_rollback is False:
                await self.conn.acommit()
            else:
                await self.conn.arollback()
        await self.conn.aclose()
        _async_connection.reset(self.token)


router = ConnectionRouter()

# For backwards compatibility. Prefer connections['default'] instead.
connection = ConnectionProxy(connections, DEFAULT_DB_ALIAS)


# Register an event to reset saved queries when a Django request is started.
def reset_queries(**kwargs):
    for conn in connections.all(initialized_only=True):
        conn.queries_log.clear()


signals.request_started.connect(reset_queries)


# Register an event to reset transaction state and close connections past
# their lifetime.
def close_old_connections(**kwargs):
    for conn in connections.all(initialized_only=True):
        conn.close_if_unusable_or_obsolete()


signals.request_started.connect(close_old_connections)
signals.request_finished.connect(close_old_connections)
