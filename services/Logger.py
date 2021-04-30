import asyncio
import logging
import websockets
from websockets import WebSocketClientProtocol, WebSocketException


async def consumer_handler(websocket: WebSocketClientProtocol) -> None:
    async for message in websocket:
        print('message:', message)


async def consume(hostname: str, port: int, topic) -> None:
    websocket_resource_url = f"ws://{hostname}:{port}"
    try:
        async with websockets.connect(websocket_resource_url,
                                      extra_headers={'topic': topic}) as websocket:
            await consumer_handler(websocket)
    except WebSocketException:
        logging.error("ERROR")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(consume('localhost', 4000, 'BTCUSDT_1m'))
    except KeyboardInterrupt as e:
        logging.info("keyboard interrupt")
        loop.run_forever()
    finally:
        loop.close()