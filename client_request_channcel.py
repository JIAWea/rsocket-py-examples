import asyncio
import time
from abc import abstractmethod

import rxbp
from rsocket import Payload
from rsocket import RSocket
from rxbp.acknowledgement.stopack import StopAck
from rxbp.observer import Observer
from rxbp.schedulers.threadpoolscheduler import ThreadPoolScheduler
from rxbp.typing import ElementType


class BaseSubscriber(Observer):
    @abstractmethod
    def on_next(self, elem: ElementType):
        pass

    def on_error(self, exc):
        print('Exception from server: ', exc)

    def on_completed(self):
        print('Completed from server')

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        self.subscription.request(2)


class ChannelSubscriber(BaseSubscriber):
    def on_next(self, elem: ElementType):
        try:
            values = list(elem)
        except Exception as exc:
            print("Exception: ", exc)
        else:
            for v in values:
                print('[RC] Response from server: {}, {}'.format(
                    v.data.decode('utf-8'),
                    v.metadata.decode('utf-8'),
                ))

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        self.subscription.request(20)


def handler(o: Observer, _):
    for i in range(1, 20):
        time.sleep(0.5)
        data = b'client data-' + str(i).encode("utf-8")
        metadata = b'metadata'
        # if i > 5:
        #     # test receive error frame
        #     metadata = b'error'
        if i == 5:
            # send error
            o.on_error(ValueError("test value error!"))

        ack = o.on_next([Payload(data, metadata)])
        if isinstance(ack, StopAck):
            print("stopped!")
            break
    o.on_completed()


# class Handler(BaseRequestHandler):
#     def request_channel(self, payload: Payload, inputs):
#         print("[RC] Request from client: {}, {}".format(
#             payload.data.decode('utf-8'),
#             payload.metadata.decode('utf-8'),
#         ))
#
#         inputs.subscribe(Subscriber())
#
#         publisher = rxbp.create(handler).pipe(
#             rxbp.op.subscribe_on(ThreadPoolScheduler("publisher")),
#         )
#         return publisher


async def session(reader, writer):
    # socket = RSocket(reader, writer, server=False, keep_alive=6000, max_lifetime=120000)
    socket = RSocket(reader, writer, server=False)
    payload = Payload(b'The quick brown fox', b'meta')

    pool = ThreadPoolScheduler("")

    publisher = rxbp.create(handler).pipe(
        # rxbp.op.map(lambda v: Payload(b'map ' + v.data, b'metadata')),
        rxbp.op.do_action(
            on_next=lambda v: print("sending " + v.data.decode('utf-8')),
            on_error=lambda err: print("sending error"),
            on_completed=lambda: print("send completed!")
        ),
        rxbp.op.subscribe_on(pool),
    )
    socket.request_channel(publisher).subscribe(ChannelSubscriber())

    # publisher = rxbp.interval(1).pipe(
    #     rxbp.op.map(lambda v: Payload(b'map client2 index-' + str(v).encode('utf-8'), b'metadata')),
    #     rxbp.op.do_action(
    #         on_next=lambda v: print("sending " + v.data.decode('utf-8')),
    #         on_completed=lambda: print("completed!"),
    #     ),
    #     rxbp.op.subscribe_on(pool),
    # )
    # socket.request_channel(payload, publisher).subscribe(ChannelSubscriber())

    await asyncio.sleep(1000)
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
