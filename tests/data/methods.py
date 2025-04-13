import time
import logging
import psycopg

from dispatcherd.publish import task
from dispatcherd.config import settings
from dispatcherd.factories import get_broker


logger = logging.getLogger(__name__)


@task(queue='test_channel')
def sleep_function(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel', on_duplicate='discard')
def sleep_discard(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel', on_duplicate='serial')
def sleep_serial(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel', on_duplicate='queue_one')
def sleep_queue_one(seconds=1):
    time.sleep(seconds)


@task(queue='test_channel')
def print_hello():
    print('hello world!!')


@task(queue='test_channel', bind=True)
def hello_world_binder(binder):
    print(f'Values in binder {vars(binder)}')
    print(f'Hello world, from worker {binder.worker_id} running task {binder.uuid}')


@task(queue='test_channel', timeout=1)
def task_has_timeout():
    time.sleep(5)


def get_queue_name():
    return 'test_channel'


@task(queue=get_queue_name)
def use_callable_queue():
    print('sucessful run using callable queue')


@task(queue=get_queue_name)
class RunJob:
    def run(self):
        print('successful run using callable queue with class')


@task(bind=True)
def prints_running_tasks(binder):
    r = binder.control('running')
    print(f'Obtained data on running tasks, result:\n{r}')


@task(bind=True)
def schedules_another_task(binder):
    r = binder.control('run', data={'task': 'tests.data.methods.print_hello'})
    print(f'Scheduled another task, result: {r}')



@task()
def break_connection():
    broker = get_broker('pg_notify', settings.brokers['pg_notify'])
    conn = broker.get_connection()
    with conn.cursor() as cursor:
        # idle_session_timeout
        # cursor.execute(f"SET idle_in_transaction_session_timeout = '0.1s';")
        # cursor.execute(f"SET idle_session_timeout = '0.1s';")
        cursor.execute("SELECT 1;")


    with conn.transaction():

        with conn.cursor() as cursor:
            print('Running immediate query with initial cursor')
            try:
                cursor.execute("SELECT 1;")
                print('  query worked')
            except Exception as exc:
                print(f'  query errored as expected\ntype: {type(exc)}\nstr: {str(exc)}')

            print('')
            print('sleeping - inside cursor')
            time.sleep(0.2)

            print('')
            print('')

            for i in range(3):
                print("Running another unrelated query with same cursor")
                try:
                    cursor.execute("SELECT 1;")
                    print('  query worked')
                except Exception as exc:
                    print(f'  query errored as expected\ntype: {type(exc)}\nstr: {str(exc)}')


        print('')
        print("Running query outside transaction")
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
                print('  query worked')
        except Exception as exc:
            print(f'  query errored as expected\ntype: {type(exc)}\nstr: {str(exc)}')

        print('')
        print('')
        print('sleeping - outside transaction')
        time.sleep(0.2)

        print('')

        for i in range(3):
            print("Running another unrelated query in same transaction")
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1;")  # Will timeout after 100ms
                    print('  query worked')
            except Exception as exc:
                print(f'  query errored as expected\ntype: {type(exc)}\nstr: {str(exc)}')


    print('')
    print('')
    print('sleeping - outside transaction')
    time.sleep(0.2)

    print('')

    for i in range(3):
        print("Running unrelated query")
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")  # Will timeout after 100ms
                print('  query worked')
        except Exception as exc:
            print(f'  query errored as expected\ntype: {type(exc)}\nstr: {str(exc)}')


def test_break_connection():
    from dispatcherd.config import setup

    setup(file_path='dispatcher.yml')
    break_connection()


@task()
def create_simultaneous_queries():
    "Simulate error of a query running while another is still executing"
    broker = get_broker('pg_notify', settings.brokers['pg_notify'])
    conn = broker.get_connection()
    with conn.transaction():

        from .db import advisory_lock

        try:
            with advisory_lock('foobar', conn, lock_session_timeout_milliseconds=1):
                with conn.cursor() as cursor:
                    print("Running query that will exceed the timeout (pg_sleep)...")
                    time.sleep(1)
                    # raise Exception
                    cursor.execute("SELECT pg_sleep(1);")  # Will timeout after 100ms
                    print('query worked unexpectedly')
        except Exception:
            print('ignoring exception under advisory lock')
            import traceback
            traceback.print_exc()

        time.sleep(1)

        try:
            print('getting new cursor')
            with conn.cursor() as cursor:
                logging.info("some other query")
                cursor.execute("SELECT 1;")  # Will timeout after 100ms
                print('select 1 finished')
        except:
            print('select 1 failed')
            import traceback
            traceback.print_exc()

        with conn.cursor() as cursor:
            print("Running query that sleep")
            cursor.execute("SELECT pg_sleep(2);")  # Will timeout after 100ms
            print('finished sleep')

    # import time
    # start = time.time()

    # with conn.cursor() as cursor:
    #     print("Setting short statement_timeout...")
    #     cursor.execute("SET idle_in_transaction_session_timeout = 100;")  # 100 milliseconds

    # time.sleep(0.5)

    # # Step 2: Run a query that will exceed that timeout
    # try:
    #     with conn.cursor() as cursor:
    #         logging.info("Running query that will exceed the timeout (pg_sleep)...")
    #         cursor.execute("SELECT pg_sleep(1);")  # Will timeout after 100ms
    #         print('query worked unexpectedly')
    # except psycopg.OperationalError as e:
    #     print(f"Expected psycopg error: {e}")
    #     assert "another command is already in progress" in str(e)
    # except Exception as e:
    #     print(f"Unexpected error: {e}")
    #     raise


def test_alan():
    from dispatcherd.config import setup

    setup(file_path='dispatcher.yml')
    create_simultaneous_queries()

    # try:
    #     # Run a long query
    #     cursor = conn.cursor()
    #     cursor.execute("SET statement_timeout = 100;")  # in milliseconds

    #     print("Starting long-running query...")
    #     cursor.execute("SELECT pg_sleep(4);")  # Sleep for 10 seconds (simulating a long query)

    #     # While the first query is still running, try sending another query
    #     print(f"Attempting to run a second query while the first one is still running... {time.time() - start}")
    #     try:
    #         cursor.execute("SELECT 1;")  # This should fail because the previous query is still running
    #         print(f"Second query executed successfully (unexpected). {time.time() - start}")
    #     except psycopg.OperationalError as e:
    #         print(f"OperationalError: {e}")
    #         assert "another command is already in progress" in str(e), "Expected 'another command is already in progress' error"
    #         raise

    #     # Commit the first query and cleanup
    #     conn.commit()
    #     cursor.close()
    # except Exception as e:
    #     print(f"Test failed: {e}")
    #     raise
