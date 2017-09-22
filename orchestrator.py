import os
import asyncio

import re
import rethinkdb as r

from logzero import logger

from cion_interface.service import service
from workq.orchestrator import Server

service_name = re.compile('^([^/]*/)?[^:]*:([^_]+)')

dispatch = {}


async def process_new_image(row):
    match = service_name.match(row['image-name'])
    svc = match.group(2).replace('-', '_')

    logger.debug("Starting new update work task.")
    result = await service.update(svc, row['image-name'])

    if not result:
        logger.warning(f"Update of {svc} failed.")
    else:
        logger.info(f"Update of {svc} succeed.")

dispatch['new-image'] = process_new_image


async def new_task_watch():
    db_host = os.environ.get('DATABASE_HOST')
    db_port = os.environ.get('DATABASE_PORT')

    await asyncio.sleep(4)  # Allow rethink to start up.

    r.set_loop_type('asyncio')

    conn = await r.connect(db_host, db_port)
    cursor = await r.db('cion').table('tasks').changes().run(conn)

    while await cursor.fetch_next():
        change = await cursor.next()

        logger.debug(f"Change in tasks table: {change}")
        row = change['new_val']

        # Only process new tasks
        if change['old_val'] is None and row['status'] == "ready":
            logger.debug(f"Dispatching row: {row}")
            handler = dispatch[row['event']]

            asyncio.ensure_future(handler(row))

    conn.close()


def main():
    loop = asyncio.get_event_loop()
    orchestrator = Server()
    orchestrator.enable(service)

    socket, server = orchestrator.run(addr='', port=8890)
    logger.info(f'Serving on {socket.getsockname()}')
    asyncio.ensure_future(new_task_watch(), loop=loop)
    loop.run_until_complete(server)
    loop.close()


if __name__ == '__main__':
    main()
