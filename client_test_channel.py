import asyncio
import json
import time
from abc import abstractmethod

import rxbp
from reactivestreams import Subscriber
from rsocket import Payload, BaseRequestHandler
from rsocket import RSocket
from rxbp.acknowledgement.stopack import StopAck
from rxbp.observer import Observer
from rxbp.schedulers.threadpoolscheduler import ThreadPoolScheduler
from rxbp.schedulers.threadpoolschedulerdispose import RXThreadPoolScheduler
from rxbp.typing import ElementType


class Handler(BaseRequestHandler):
    def request_fire_and_forget(self, payload: Payload):
        str_data = payload.data.decode('utf-8')
        data = json.loads(str_data)
        print("[FNF] data: ", data)


class ChannelSubscriber:
    def __init__(self):
        self.on_next_count = 0

    def on_next(self, elem: ElementType):
        self.on_next_count += 1

    def on_error(self, exc):
        print('Exception from server: ', str(exc))

    def on_completed(self):
        print("Completed! next_count: {}".format(self.on_next_count))

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        self.subscription.request(20)


async def session(reader, writer):
    socket = RSocket(reader, writer, handler_factory=Handler, server=False)

    def handler(o: Observer, _):
        for i in range(1, 20):
            time.sleep(0.5)
            data, metadata = b'1', b''
            ack = o.on_next([Payload(data, metadata)])
            if isinstance(ack, StopAck):
                break
        o.on_completed()

    socket.request_channel(
        rxbp.create(handler).pipe(
            rxbp.op.subscribe_on(pool),
        )
    ).subscribe(ChannelSubscriber())

    await asyncio.sleep(1000)
    await socket.close()
    return


if __name__ == '__main__':
    pool = RXThreadPoolScheduler()

    loop = asyncio.get_event_loop()
    try:
        connection = loop.run_until_complete(asyncio.open_connection(
            'localhost', 9898))
        loop.run_until_complete(session(*connection))
    finally:
        loop.close()
