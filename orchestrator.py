import asyncio

from logzero import logger

from cion_interface.service import service
from workq.orchestrator import Server


async def test():
    while True:
        asyncio.ensure_future(start_task())
        await asyncio.sleep(10)


async def start_task():
    result = await service.update("test_web", "test:latest")
    logger.info(result)


def main():
    loop = asyncio.get_event_loop()
    orchestrator = Server()
    orchestrator.enable(service)

    socket, server = orchestrator.run(addr='', port=8890)
    logger.info(f'Serving on {socket.getsockname()}')
    loop.create_task(test())
    loop.run_until_complete(server)
    loop.close()


if __name__ == '__main__':
    main()
