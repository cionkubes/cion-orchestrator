import functools
import os
import asyncio

from aioreactive.operators import concat
from aioreactive.core import Operators, AsyncAnonymousObserver, subscribe
from async_rethink import connection, Connection

from logzero import logger, loglevel
loglevel(int(os.environ.get("LOGLEVEL", 10)))

from cion_interface.service import service
from workq.orchestrator import Server
from rethinkdb import r

dispatch = {}


async def gather(async_generator):
    result = []

    async for element in async_generator:
        result.append(element)

    return result


def set_status(conn, row, status):
    return conn.db().table('tasks').get(row['id']).update({
        'status': status,
        'time': r.now().to_epoch_time()
    })


def update_service(conn, swarm, service, image):
    return conn.db().table('tasks').insert({
        'swarm': swarm,
        'service': service,
        'image-name': image,
        'event': 'service-update',
        'status': 'ready',
        'time': r.now().to_epoch_time()
    })


def handler(handler_fn):
    @functools.wraps(handler_fn)
    async def wrapper(conn: Connection, row, *args, **kwargs):
        try:
            await conn.run(set_status(conn, row, 'processing'))
            await handler_fn(conn, row, *args, **kwargs)
            await conn.run(set_status(conn, row, 'done'))
        except:
            logger.exception(
                f"Unknown exception in task handler {handler_fn.__name__}")
            await conn.run(set_status(conn, row, 'erroneous'))

    return wrapper


@handler
async def process_new_image(conn, row):
    logger.debug("Starting new distribute to task.")

    image = row['image-name']

    targets = await service.distribute_to(image)
    for swarm, svc, image in targets:
        await conn.run(update_service(conn, swarm, svc, image))

dispatch['new-image'] = process_new_image


@handler
async def process_service_update(conn, row):
    logger.debug("Starting new update work task.")

    swarm = row['swarm']
    svc = row['service']
    image = row['image-name']

    await service.update(swarm, svc, image)

dispatch["service-update"] = process_service_update


async def new_task_watch():
    db_host = os.environ.get('DATABASE_HOST')
    db_port = os.environ.get('DATABASE_PORT')

    conn = await connection(db_host, db_port)

    async def new_change(change):
        try:
            logger.debug(f"Dispatching row: {change}")
            handler = dispatch[change['event']]

            await handler(conn, change)
        except KeyError:
            logger.debug(f"No handler for event type {change['event']}")
        except Exception:
            logger.exception("Unknown exception in task processing")


    start_with = conn.observable_query(conn.db().table('tasks') \
        .filter(lambda row: row['status'] == 'ready')
    )

    ready_tasks = concat(
        start_with,
        conn.observe("tasks")
            | Operators.map(lambda c: c['new_val'])
            | Operators.filter(lambda task: task['status'] == "ready")
    )

    return await subscribe(ready_tasks, AsyncAnonymousObserver(new_change))


def main():
    loop = asyncio.get_event_loop()
    orchestrator = Server()
    orchestrator.enable(service)

    socket, server = orchestrator.run(addr='', port=8890)
    logger.info(f'Serving on {socket.getsockname()}')
    loop.run_until_complete(new_task_watch())
    loop.run_until_complete(server)
    loop.close()


if __name__ == '__main__':
    main()
