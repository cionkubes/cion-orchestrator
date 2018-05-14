import functools
import asyncio
import os

from collections import defaultdict
from datetime import timezone, datetime

from rethinkdb import r
from aioreactive.operators import concat
from aioreactive.core import Operators, AsyncAnonymousObserver, subscribe
from async_rethink import connection, Connection
from async_rethink.reactive import with_latest_from, AsyncRepeatedlyCallWithLatest

from logzero import logger, loglevel
loglevel(int(os.environ.get("LOGLEVEL", 10)))

from cion_interface.service import service
from workq.orchestrator import Server

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


def new_task(conn, event, task):
    return conn.db().table('tasks').insert({
        **task,
        'event': event,
        'status': 'ready',
        'time': r.now().to_epoch_time()
    })


def update_service(conn, environment, service, image):
    return new_task(conn, "service-update", {
        'environment': environment,
        'service': service,
        'image-name': image,
    })


def remove_delayed(conn, task):
    return conn.db().table('delayed_tasks').get(task['id']).delete()


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

    environment = row['environment']
    svc = row['service']
    image = row['image-name']

    await service.update(environment, svc, image)

dispatch["service-update"] = process_service_update


def group_by(key):
    def inner(xs):
        result = defaultdict(list)
        for x in xs:
            result[x[key]].append(x)

        return dict(result)
    return inner


async def new_task_watch():
    db_host = os.environ.get('DATABASE_HOST')
    db_port = os.environ.get('DATABASE_PORT')

    conn = await connection(db_host, db_port)

    async def new_change(arg):
        try:
            row, webhooks = arg[0], arg[1]
            logger.debug(f"Dispatching row: {row}")

            event = row.get('event', 'undefined')
            hooks = webhooks.get(event, [])
            if len(hooks) > 0:
                webhook_future = asyncio.ensure_future(service.webhook(hooks, row))

            try:
                handler = dispatch[event]
                await handler(conn, row)
            except KeyError:
                logger.debug(f"No handler for event type {row['event']}")
            except:
                logger.exception("Unknown exception in task processing")

            try:
                if len(hooks) > 0:
                    await webhook_future
            except:
                logger.exception("Unknown exception in webhook.")
        except:
            logger.exception("Unknown exception in task watch.")

    async def delayed(tasks):
        logger.debug(f"Got update in delayed tasks: {tasks}")

        if len(tasks) == 0:
            await asyncio.sleep(60)
            return

        task = next(iter(tasks))

        delta = datetime.fromtimestamp(task['at'], timezone.utc) - datetime.now(timezone.utc)
        await asyncio.sleep(delta.total_seconds())

        await conn.run(new_task(conn, task['event'], task['parameters']))
        await asyncio.shield(conn.run(remove_delayed(conn, task)))


    ready_tasks = conn.start_with_and_changes(conn.db().table('tasks') \
        .filter(lambda row: row['status'] == 'ready')
    )   | Operators.map(lambda c: c['new_val'])\
        | Operators.filter(lambda x: x is not None)

    webhooks = conn.changes_accumulate(conn.db().table('webhooks'))\
        | Operators.map(group_by("event"))

    return await asyncio.gather(
        subscribe(
            ready_tasks | with_latest_from(webhooks),
            AsyncAnonymousObserver(new_change)
        ),
        subscribe(
            conn.changes_accumulate(conn.db().table('delayed_tasks'))\
                | Operators.map(lambda tasks: sorted(tasks, key=lambda task: task['at'])),
            AsyncRepeatedlyCallWithLatest(delayed)
        )
    )


def main():
    loop = asyncio.get_event_loop()
    orchestrator = Server()
    orchestrator.enable(service)

    socket, server = orchestrator.run(addr='', port=8890)
    logger.info(f'Serving on {socket.getsockname()}')
    dispose_of = loop.run_until_complete(new_task_watch())
    loop.run_until_complete(server)
    loop.run_until_complete(asyncio.gather(*[sub.adispose() for sub in dispose_of]))
    loop.close()


if __name__ == '__main__':
    main()
