import asyncio
import logging
import json
import websockets
import yaml
from websockets import WebSocketClientProtocol, ConnectionClosed


logging.basicConfig(level='INFO')


def read_config(filename='config.yaml'):
    with open(filename) as file:
        return yaml.full_load(file)


class SimpleTransportHub:
    clients = set()
    topic_clients = dict()

    async def register(self, ws: WebSocketClientProtocol) -> None:
        topic = 'all'
        if 'topic' in ws.request_headers.keys():
            topic = ws.request_headers['topic']
        if topic not in self.topic_clients.keys():
            self.topic_clients[topic] = {ws}
        else:
            self.topic_clients[topic].add(ws)
        print(ws.remote_address, ' connected')

    async def unregister(self, ws: WebSocketClientProtocol) -> None:
        topic = 'all'
        if 'topic' in ws.request_headers.keys():
            topic = ws.request_headers['topic']
        if topic in self.topic_clients.keys():
            self.topic_clients[topic].discard(ws)
        print(ws.remote_address, ' disconnected')

    async def ws_handler(self, ws: WebSocketClientProtocol, uri: str) -> None:
        await self.register(ws)
        try:
            await self.distribute(ws)
        except ConnectionClosed:
            logging.info('connection suddenly closed')
        finally:
            await self.unregister(ws)

    async def distribute(self, ws: WebSocketClientProtocol) -> None:
        async for message in ws:
            await self.send_to_clients(message)

    async def send_to_clients(self, message: str) -> None:
        if self.topic_clients:
            topic = await self.get_message_topic(message)
            clients = self.topic_clients.get(topic)
            if clients:
                await asyncio.wait([client.send(message) for client in clients])

    async def get_message_topic(self, message: str) -> str:
        result = 'all'
        try:
            json_obj = json.loads(message)
            result = json_obj.get('topic')
        except ValueError:
            logging.error(f'incorrect message format: {message}')
        finally:
            return result


config = read_config()
server = SimpleTransportHub()
start_server = websockets.serve(server.ws_handler, config['transport_hub']['host'], config['transport_hub']['port'])
loop = asyncio.get_event_loop()
loop.run_until_complete(start_server)
loop.run_forever()


