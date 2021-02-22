import asyncio

from rsocket import RSocket, BaseRequestHandler
from rsocket.payload import Payload


class Handler(BaseRequestHandler):
    def request_fire_and_forget(self, payload: Payload):
        print("[FNF] Received from client: {}, {}".format(
            payload.data.decode('utf-8'),
            payload.metadata.decode('utf-8'),
        ))

    def request_response(self, payload: Payload) -> asyncio.Future:
        data = payload.data.decode('utf-8')
        metadata = payload.metadata.decode('utf-8')
        print("[RR] Received from client: {}, {}".format(data, metadata))

        future = loop.create_future()
        if metadata == "error":
            future.set_exception(ValueError("Value error, got: {}".format(metadata)))
        else:
            future.set_result(Payload(b"server " + payload.data, b"server " + payload.metadata))
        return future


def session(reader, writer):
    RSocket(reader, writer, handler_factory=Handler)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    service = loop.run_until_complete(asyncio.start_server(
        session, 'localhost', 9898))
    try:
        loop.run_forever()
    finally:
        service.close()
        loop.close()
