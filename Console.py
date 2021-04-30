import asyncio
import logging
import websockets
from websockets import WebSocketClientProtocol

logging.basicConfig(level='INFO')


async def consumer_handler(websocket: WebSocketClientProtocol) -> None:
    async for message in websocket:
        await process_message(message)


async def process_message(message):
    print(message)


async def consume(hostname: str, port: int, topic: str) -> None:
    websocket_resource_url = f"ws://{hostname}:{port}"
    while True:
        try:
            async with websockets.connect(websocket_resource_url,
                                          extra_headers={'topic': topic}) as websocket:
                await consumer_handler(websocket)
        except BaseException:
            logging.error("error connection to hub")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume('localhost', 4000, 'BTCUSDT'))
