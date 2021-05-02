import asyncio
import logging
import websockets
import yaml
from websockets import WebSocketClientProtocol

logging.basicConfig(level='INFO')


def read_config(filename='config.yaml'):
    with open(filename) as file:
        return yaml.full_load(file)


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
    config = read_config()
    symbol = config['binance_pair']['symbol']
    host = config['transport_hub']['host']
    port = config['transport_hub']['port']
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume(host, port, symbol))
