import asyncio
from abc import abstractmethod

from rsocket import Payload
from rsocket import RSocket
from rxbp.acknowledgement.stopack import StopAck
from rxbp.observer import Observer
from rxbp.typing import ElementType


class BaseSubscriber(Observer):
    @abstractmethod
    def on_next(self, elem: ElementType):
        pass

    def on_error(self, exc):
        print('Exception from server: exception: ', exc)

    def on_completed(self):
        print('Completed from server')

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        self.subscription.request(2)


class StreamSubscriber(BaseSubscriber):
    def __init__(self):
        self.count = 10

    def on_next(self, elem: ElementType):
        try:
            values = list(elem)
        except Exception as exc:
            print("Exception: ", exc)
        else:
            for v in values:
                print('[RS] Response from server: {}, {}'.format(
                    v.data.decode('utf-8'),
                    v.metadata.decode('utf-8')
                ))

            self.count -= 1
            if self.count == 0:
                self.subscription.cancel()

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        self.subscription.request(20)


def handler(o: Observer, _):
    for i in range(1, 20):
        # time.sleep(0.5)
        ack = o.on_next([Payload(b'server data-' + str(i).encode("utf-8"), b'metadata')])
        if isinstance(ack, StopAck):
            print("stopped!")
            break
    o.on_completed()


async def session(reader, writer):
    # socket = RSocket(reader, writer, server=False, keep_alive=6000, max_lifetime=120000)
    socket = RSocket(reader, writer, server=False)
    payload = Payload(b'The quick brown fox', b'meta')

    socket.request_stream(payload).subscribe(StreamSubscriber())

    await asyncio.sleep(100)
    await socket.close()
    return payload


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        connection = loop.run_until_complete(asyncio.open_connection(
            'localhost', 9898))
        loop.run_until_complete(session(*connection))
    finally:
        loop.close()
