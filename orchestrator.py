import functools
import os
import asyncio

from async_rethink import connection, Connection, gather

from logzero import logger, loglevel
loglevel(int(os.environ.get("LOGLEVEL", 10)))

from cion_interface.service import service
from workq.orchestrator import Server

dispatch = {}


def set_status(conn, row, status):
    return conn.db().table('tasks').get(row['id']).update({'status': status})


def handler(handler_fn):
    @functools.wraps
    async def wrapper(conn: Connection, row, *args, **kwargs):
        try:
            await conn.run(set_status(conn, row, 'processing'))
            await handler_fn(row, *args, **kwargs)
            await conn.run(set_status(conn, row, 'done'))
        except:
            logger.exception(f"Unknown exception in task handler {handler_fn.__name__}")
            await conn.run(set_status(conn, row, 'erroneous'))

    return wrapper


@handler
async def process_new_image(row):
    image = row['image-name']

def handler(handler_fn):
    @functools.wraps(handler_fn)
    async def wrapper(conn: Connection, row, *args, **kwargs):
        try:
            await conn.run(set_status(conn, row, 'processing'))
            await handler_fn(row, *args, **kwargs)
            await conn.run(set_status(conn, row, 'done'))
        except:
            logger.exception(f"Unknown exception in task handler {handler_fn.__name__}")
            await conn.run(set_status(conn, row, 'erroneous'))

    return wrapper


@handler
async def process_new_image(row):
    image = row['image-name']

    logger.debug("Starting new update work task.")
    targets = await service.distribute_to(image)
    await asyncio.gather(service.update(swarm, svc, image) for swarm, svc in targets)


dispatch['new-image'] = process_new_image


async def new_task_watch():
    db_host = os.environ.get('DATABASE_HOST')
    db_port = os.environ.get('DATABASE_PORT')

    conn = await connection(db_host, db_port)

    def new_change(change):
        try:
            logger.debug(f"Dispatching row: {change}")
            handler = dispatch[change['event']]

            asyncio.ensure_future(handler(conn, change))
        except Exception:
            logger.exception("Unknown exception in task processing")

    ready_tasks = conn.run_iter(conn.db().table('tasks').filter(lambda row: row['status'] == 'ready'))
    unprocessed = await gather(ready_tasks)

    return conn.observe('tasks')\
        .filter(lambda c: c['old_val'] is None)\
        .map(lambda c: c['new_val'])\
        .start_with(*unprocessed)\
        .subscribe(new_change)


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
