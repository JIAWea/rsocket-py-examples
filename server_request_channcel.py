import asyncio
import time

import rxbp
from reactivestreams.publisher import Publisher
from rsocket import RSocket, BaseRequestHandler
from rsocket.payload import Payload
from rxbp.acknowledgement.stopack import StopAck
from rxbp.observer import Observer
from rxbp.schedulers.threadpoolscheduler import ThreadPoolScheduler
from rxbp.typing import ElementType


class ChannelSubscriber(Observer):
    def on_next(self, elem: ElementType):
        try:
            values = list(elem)
        except Exception as exc:
            print("Exception: ", exc)
        else:
            for v in values:
                data = v.data.decode('utf-8')
                metadata = v.metadata.decode('utf-8')
                print("[RC] Received from client: {}, {}".format(data, metadata))

    def on_error(self, exc):
        print('Exception from client: exception: ', exc)

    def on_completed(self):
        print('Completed from client')

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        self.subscription.request(100)


def handler(o: Observer, _):
    for i in range(1, 20):
        time.sleep(0.5)
        ack = o.on_next([Payload(b'server data-' + str(i).encode("utf-8"), b'error')])
        # if i == 5:
        #     o.on_error(ValueError("my error"))
        print("i: ", i)
        if isinstance(ack, StopAck):
            print("stopped!")
            break

    o.on_completed()


class Handler(BaseRequestHandler):
    def request_channel(self, inputs: Publisher):
        inputs.subscribe(ChannelSubscriber())

        pool = ThreadPoolScheduler("publisher")

        publisher = rxbp.create(handler).pipe(
            rxbp.op.do_action(
                on_next=lambda v: print("sending " + v.data.decode('utf-8')),
                on_completed=lambda: print("completed!"),
            ),
            rxbp.op.subscribe_on(pool),
        )

        return publisher


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
