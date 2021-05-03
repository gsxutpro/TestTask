import asyncio
import logging
import websockets
import yaml
import json
from websockets import WebSocketClientProtocol

logging.basicConfig(level='INFO')


def read_config(filename='config.yaml'):
    with open(filename) as file:
        return yaml.full_load(file)


async def consumer_handler(websocket: WebSocketClientProtocol) -> None:
    async for message in websocket:
        await process_message(message)


async def process_message(message):
    """
    Обработка поступившего сообщения

    :param message: строка в формате JSON
    :return: строка в требуемом формате
    """
    print(convert_message_to_str(message))


def convert_message_to_str(message):
    """
    Преобразование входного сообщений в формате JSON в строку, требуемого формата

    :param message: строка в формате JSON
    :return: строка в требуемом формате
    """
    try:
        msg_dict = json.loads(message)
    except ValueError:
        logging.error("error converting message to json")
    # timestamp: 2021-04-27 12:03; open: 55000; low: 54990; high: 55023; close: 55019
    res = f"timestamp: {msg_dict['E']}; symbol: {msg_dict['s']}; price: {float(msg_dict['p']):.2f}; volume: " \
          f"{float(msg_dict['q']):.5f}"
    return res


async def consume(hostname: str, port: int, topic: str) -> None:
    """
    Подключение к transport hub. Через headers передается topic, который желает получать данный сервис - потребитель.

    :param hostname: хост transport hub
    :param port: порт transport hub
    :param topic: имя канала, на который подписывается данный сервис
    """
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
