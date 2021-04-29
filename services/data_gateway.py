import logging
from binance.client import Client
from binance.websockets import BinanceSocketManager
from time import sleep
from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketClientFactory, WebSocketClientProtocol, connectWS
from twisted.internet.protocol import ReconnectingClientFactory
import json
from twisted.internet import defer
defer.setDebugging(True)

logging.basicConfig(level="INFO")


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
    try:
        json_message = json.dumps(msg)
        if "state" in ProducerClientProtocol().__dict__.keys() \
                and ProducerClientProtocol().state == ProducerClientProtocol.STATE_OPEN:
            ProducerClientProtocol().sendMessage(bytes(json_message.encode('utf8')))
    except:
        logging.error('error process binance message')



def serve():
    api_key = 'jf6jbytIakmjQihPJCK0BegeqHkWDA1FlavLHszKohpIcQzQmSKPyUjaBmvnJyNs'
    api_secret = 'vG22RvWl45kdBlWq9DGexkmKZwqjRjC9MmXr7VPF38yUrcU1yEXtQZuVwkz9KUrI'

    client = Client(api_key, api_secret)
    bm = BinanceSocketManager(client)

    conn_key = bm.start_trade_socket('BTCUSDT', process_message)

    bm.start()
    sleep(130)
    bm.stop_socket(conn_key)
    bm.close()


def connect_to_transport_hub(host, port):
    try:
        if True:
            factory = TestTDClientFactory(f"ws://{host}:{port}")
            factory.protocol = ProducerClientProtocol
            connectWS(factory)
    except Exception:
        print('error connecting to transport hub')


if __name__ == '__main__':
    #lc = LoopingCall(connect_to_transport_hub, 'localhost', 4000)
    #lc.start(1)
    connect_to_transport_hub('localhost', 4000)
    serve()
    reactor.stop()
