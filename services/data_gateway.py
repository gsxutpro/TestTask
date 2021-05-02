import logging
import json
import yaml
from datetime import datetime
from autobahn.twisted.websocket import WebSocketClientFactory, WebSocketClientProtocol, connectWS
from twisted.internet.protocol import ReconnectingClientFactory
from binance.websockets import BinanceSocketManager
from binance.client import Client


logging.basicConfig(level="INFO")
trades = {}
client = None


def read_config(filename='config.yaml'):
    with open(filename) as file:
        return yaml.full_load(file)


class ProducerClientProtocol(WebSocketClientProtocol):

    def onOpen(self):
        global client
        client = self
        logging.info(msg='connected to transport hub')

    def onConnect(self, response):
        self.factory.resetDelay()


class TestTDClientFactory(ReconnectingClientFactory, WebSocketClientFactory):

    protocol = ProducerClientProtocol

    maxDelay = 10
    maxRetries = 5

    def startedConnecting(self, connector):
        print('Started to connect.')

    def clientConnectionLost(self, connector, reason):
        print('Lost connection. Reason: {}'.format(reason))
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        print('Connection failed. Reason: {}'.format(reason))
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)


def process_message(msg):
    # сохранение и аггрегация данных для ohlc за период
    aggregate_ohlc(msg)
    # в качестве топика сообщения используется значение торговаой пары
    msg['topic'] = str(msg['s'])
    send_message(msg)


def store_trade(msg):
    keys = ['s', 'E', 'p', 'q', 'T']
    to_store = {k: v for k, v in msg.items() if k in keys}
    to_store['p'] = float(to_store['p'])
    to_store['dt'] = datetime.fromtimestamp(int(to_store['T']) / 1000)
    symbol = msg['s']
    if symbol in trades.keys():
        trades[symbol].append(to_store)
    else:
        trades[symbol] = [to_store]


def aggregate_ohlc(msg):
    # сохранение сделки
    store_trade(msg)
    # аггрегирование данных сделок за 1 минуту по серверному времени
    for s, tr in trades.items():
        if check_1m(tr):
            last_minute = tr[-2]['dt'].minute
            tr_filt = list(filter(lambda x: x['dt'].minute == last_minute, trades[s]))
            res_dict = get_ohlc_volume(tr_filt, '1m')
            last_trade = tr[-1]
            trades[s]=[last_trade]
            send_message(res_dict)


def get_ohlc_volume(lst, period):
    prices = [x['p'] for x in lst]
    res = {'topic': f"{lst[0]['s']}_{str(period)}", 'volume': sum([float(x['q']) for x in lst]), 'open': prices[0],
           'high': max(prices), 'low': min(prices), 'close': prices[-1], 'dt': lst[0]['dt'].strftime("%Y-%m-%d %H:%M")}
    return res


def check_1m(tr):
    if len(tr) < 2:
        return False
    minute1 = tr[-1]['dt'].minute
    minute2 = tr[-2]['dt'].minute
    return minute1 != minute2


def send_message(msg):
    try:
        json_message = json.dumps(msg)
        global client
        if client is not None:
            if "state" in client.__dict__.keys() \
                    and client.state == ProducerClientProtocol.STATE_OPEN:
                client.sendMessage(bytes(json_message.encode('utf8')))
    except ValueError:
        logging.error('error process binance message')


def connect_to_transport_hub(host, port):
    try:
        factory = TestTDClientFactory(f"ws://{host}:{port}")
        connectWS(factory)
    except BaseException:
        print('error connecting to transport hub')


def serve(symbol, api_key, api_secret):
    binance_client = Client(api_key, api_secret)
    bm = BinanceSocketManager(binance_client)
    bm.start_trade_socket(symbol, process_message)
    # запуск
    bm.start()


if __name__ == '__main__':
    config = read_config()
    api_key = config['binance_api']['api_key']
    api_secret = config['binance_api']['api_secret']
    symbol = config['binance_pair']['symbol']
    host = config['transport_hub']['host']
    port = config['transport_hub']['port']
    connect_to_transport_hub(host, port)
    serve(symbol, api_key, api_secret)

