import os
import asyncio

import re
from async_rethink import connection

from logzero import logger, loglevel
loglevel(int(os.environ.get("LOGLEVEL", 10)))

from cion_interface.service import service
from workq.orchestrator import Server

service_name = re.compile('^([^/]*/)?[^:]*:([^_]+)')

dispatch = {}


async def process_new_image(row):
    image = row['image-name']

    logger.debug("Starting new update work task.")
    targets = await service.distribute_to(image)

    for swarm, svc in targets:
        await service.update(swarm, svc, image)

dispatch['new-image'] = process_new_image


async def new_task_watch():
    db_host = os.environ.get('DATABASE_HOST')
    db_port = os.environ.get('DATABASE_PORT')

    conn = await connection(db_host, db_port)

    def new_change(change):
        try:
            logger.debug(f"Dispatching row: {change}")
            handler = dispatch[change['event']]

            asyncio.ensure_future(handler(change))
        except Exception:
            logger.exception("Unknown exception in task processing")

    return conn.observe('tasks')\
        .filter(lambda c: c['old_val'] is None)\
        .map(lambda c: c['new_val'])\
        .subscribe(new_change)


def main():
    loop = asyncio.get_event_loop()
    orchestrator = Server()
    orchestrator.enable(service)

    socket, server = orchestrator.run(addr='', port=8890)
    logger.info(f'Serving on {socket.getsockname()}')
    s = asyncio.ensure_future(server)
    loop.run_until_complete(new_task_watch())
    loop.run_until_complete(s)
    loop.close()


if __name__ == '__main__':
    main()
