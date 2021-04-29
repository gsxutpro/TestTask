import asyncio
import logging
import websockets
from websockets import WebSocketClientProtocol, ConnectionClosed

logging.basicConfig(level='INFO')


class SimpleTransportHub:
    clients = set()

    async def register(self, ws: WebSocketClientProtocol,) -> None:
        self.clients.add(ws)
        print(ws.remote_address, ' connected')

    async def unregister(self, ws: WebSocketClientProtocol) -> None:
        self.clients.remove(ws)
        print(ws.remote_address, ' disconnected')

    async def send_to_clients(self, message: str) -> None:
        if self.clients:
            await asyncio.wait([client.send(message) for client in self.clients])

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


server = SimpleTransportHub()
start_server = websockets.serve(server.ws_handler, 'localhost', 4000)
loop = asyncio.get_event_loop()
loop.run_until_complete(start_server)
loop.run_forever()


