import logging
from binance.client import Client
from binance.websockets import BinanceSocketManager
from time import sleep
from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketClientFactory, WebSocketClientProtocol, connectWS
from twisted.internet.protocol import ReconnectingClientFactory
import json
from datetime import datetime


logging.basicConfig(level="INFO")
trades = {}


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ProducerClientProtocol(WebSocketClientProtocol,  metaclass=Singleton):
    def onOpen(self):
        logging.info(msg='connected to transport hub')


class TestTDReconnectingClientFactory(ReconnectingClientFactory):

    initialDelay = 0.1

    maxDelay = 10

    maxRetries = 5


class TestTDClientFactory(WebSocketClientFactory, TestTDReconnectingClientFactory):

    protocol = ProducerClientProtocol
    _reconnect_error_payload = {
        'e': 'error',
        'm': 'Max reconnect retries reached'
    }

    def clientConnectionFailed(self, connector, reason):
        self.retry(connector)
        if self.retries > self.maxRetries:
            self.callback(self._reconnect_error_payload)

    def clientConnectionLost(self, connector, reason):
        self.retry(connector)
        if self.retries > self.maxRetries:
            self.callback(self._reconnect_error_payload)


def process_message(msg):
    # сохранение и аггрегация данных для ohlc за период
    aggregate_ohlc(msg)
    # в качестве топика сообщения используется значение торговаой пары
    msg['topic'] = str(msg['s'])
    send_message(msg)


def store_trade(msg):
    keys = ['s', 'E', 'p', 'q', 'T']
    to_store = {k: v for k,v in msg.items() if k in keys}
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
            send_message(res_dict)


def get_ohlc_volume(lst, period):
    prices = [x['p'] for x in lst]
    res = {'topic': f"{lst[0]['s']}_{str(period)}", 'volume': sum([float(x['q']) for x in lst]), 'open': prices[0],
           'high': max(prices), 'low': min(prices), 'close': prices[-1], 'dt': lst[0]['dt'].strftime("%d/%m/%Y, %H:%M")}
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
        if "state" in ProducerClientProtocol().__dict__.keys() \
                and ProducerClientProtocol().state == ProducerClientProtocol.STATE_OPEN:
            ProducerClientProtocol().sendMessage(bytes(json_message.encode('utf8')))
    except ValueError:
        logging.error('error process binance message')


def serve():
    api_key = 'jf6jbytIakmjQihPJCK0BegeqHkWDA1FlavLHszKohpIcQzQmSKPyUjaBmvnJyNs'
    api_secret = 'vG22RvWl45kdBlWq9DGexkmKZwqjRjC9MmXr7VPF38yUrcU1yEXtQZuVwkz9KUrI'

    client = Client(api_key, api_secret)
    bm = BinanceSocketManager(client)

    conn_key = bm.start_trade_socket('BTCUSDT', process_message)

    bm.start()
    # sleep(130)
    # bm.stop_socket(conn_key)
    # bm.close()


def connect_to_transport_hub(host, port):
    try:
        if True:
            factory = TestTDClientFactory(f"ws://{host}:{port}")
            factory.protocol = ProducerClientProtocol
            connectWS(factory)
    except BaseException:
        print('error connecting to transport hub')


if __name__ == '__main__':
    connect_to_transport_hub('localhost', 4000)
    serve()
    # reactor.stop()
