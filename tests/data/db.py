from contextlib import contextmanager
from zlib import crc32


# NOTE: the django_pglocks_advisory_lock context manager was forked from the django-pglocks v1.0.4
# that was licensed under the MIT license


@contextmanager
def django_pglocks_advisory_lock(lock_id, connection, shared=False, wait=True, using=None):

    # Assemble the function name based on the options.

    function_name = 'pg_'

    if not wait:
        function_name += 'try_'

    function_name += 'advisory_lock'

    if shared:
        function_name += '_shared'

    release_function_name = 'pg_advisory_unlock'
    if shared:
        release_function_name += '_shared'

    # Format up the parameters.

    tuple_format = False

    if isinstance(
        lock_id,
        (
            list,
            tuple,
        ),
    ):
        if len(lock_id) != 2:
            raise ValueError("Tuples and lists as lock IDs must have exactly two entries.")

        if not isinstance(lock_id[0], int) or not isinstance(lock_id[1], int):
            raise ValueError("Both members of a tuple/list lock ID must be integers")

        tuple_format = True
    elif isinstance(lock_id, str):
        # Generates an id within postgres integer range (-2^31 to 2^31 - 1).
        # crc32 generates an unsigned integer in Py3, we convert it into
        # a signed integer using 2's complement (this is a noop in Py2)
        pos = crc32(lock_id.encode("utf-8"))
        lock_id = (2**31 - 1) & pos
        if pos & 2**31:
            lock_id -= 2**31
    elif not isinstance(lock_id, int):
        raise ValueError("Cannot use %s as a lock id" % lock_id)

    if tuple_format:
        base = "SELECT %s(%d, %d)"
        params = (
            lock_id[0],
            lock_id[1],
        )
    else:
        base = "SELECT %s(%d)"
        params = (lock_id,)

    acquire_params = (function_name,) + params

    command = base % acquire_params
    cursor = connection.cursor()

    cursor.execute(command)

    if not wait:
        acquired = cursor.fetchone()[0]
    else:
        acquired = True

    try:
        yield acquired
    finally:
        if acquired:
            release_params = (release_function_name,) + params

            command = base % release_params
            cursor.execute(command)

        cursor.close()


@contextmanager
def advisory_lock(lock_id, connection, lock_session_timeout_milliseconds=0, **kwargs):
    """Context manager that wraps the pglocks advisory lock

    This obtains a named lock in postgres, idenfied by the args passed in
    usually the lock identifier is a simple string.

    @param: wait If True, block until the lock is obtained
    @param: shared Whether or not the lock is shared
    @param: lock_session_timeout_milliseconds Postgres-level timeout
    @param: using django database identifier
    """
    cur = None
    idle_in_transaction_session_timeout = None
    idle_session_timeout = None
    if lock_session_timeout_milliseconds > 0:
        with connection.cursor() as cur:
            idle_in_transaction_session_timeout = cur.execute("SHOW idle_in_transaction_session_timeout;").fetchone()[0]
            idle_session_timeout = cur.execute("SHOW idle_session_timeout;").fetchone()[0]
            print('Setting idle settings')
            print(f"SET idle_session_timeout = '{lock_session_timeout_milliseconds}s';")
            cur.execute(f"SET idle_in_transaction_session_timeout = '{lock_session_timeout_milliseconds}s';")
            cur.execute(f"SET idle_session_timeout = '{lock_session_timeout_milliseconds}s';")
    with django_pglocks_advisory_lock(lock_id, connection, **kwargs) as internal_lock:
        yield internal_lock
        if lock_session_timeout_milliseconds > 0:
            with connection.cursor() as cur:
                print('Resetting idle settings back to prior')
                cur.execute(f"SET idle_in_transaction_session_timeout = '{idle_in_transaction_session_timeout}s';")
                cur.execute(f"SET idle_session_timeout = '{idle_session_timeout}s';")
