import json
import asyncio
import logging
import websockets
import yaml
from websockets import WebSocketClientProtocol, WebSocketException
from datetime import datetime


log_filename = f"{datetime.now().strftime('%d%m%Y_%H%M')}.txt"
logging.basicConfig(filename=log_filename,
                    filemode='a',
                    format='%(message)s',
                    level=logging.INFO)


def read_config(filename='config.yaml'):
    with open(filename) as file:
        return yaml.full_load(file)


async def consumer_handler(websocket: WebSocketClientProtocol) -> None:
    async for message in websocket:
        logging.info(convert_message_to_str(message))


def convert_message_to_str(message: str) -> str:
    """
    Преобразование входного сообщений в формате JSON в строку, требуемого формата.

    :param message: строка в формате JSON
    :return: строка в требуемом формате
    """
    msg_dict = json.loads(message)
    # timestamp: 2021-04-27 12:03; open: 55000; low: 54990; high: 55023; close: 55019
    res = f"timestamp: {msg_dict['dt']}; open: {msg_dict['open']:.0f}; low: {msg_dict['low']:.0f}; high: " \
          f"{msg_dict['high']:.0f}; close: {msg_dict['close']:.0f}"
    return res


async def consume(hostname: str, port: int, topic) -> None:
    """
    Подключение к transport hub. Через headers передается topic, который желает получать данный сервис - потребитель.

    :param hostname: хост transport hub
    :param port: порт transport hub
    :param topic: имя канала, на который подписывается данный сервис
    """
    websocket_resource_url = f"ws://{hostname}:{port}"
    try:
        async with websockets.connect(websocket_resource_url,
                                      extra_headers={'topic': topic}) as websocket:
            await consumer_handler(websocket)
    except WebSocketException:
        print("error connecting to transport hub")


if __name__ == '__main__':
    config = read_config()
    loop = asyncio.get_event_loop()
    symbol_period = config['binance_pair']['symbol_period']
    host = config['transport_hub']['host']
    port = config['transport_hub']['port']
    try:
        loop.run_until_complete(consume(host, port, symbol_period))
    except KeyboardInterrupt as e:
        print("keyboard interrupt")
        loop.run_forever()
    finally:
        loop.close()
