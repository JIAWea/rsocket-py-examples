import asyncio
import time

import rxbp
from rsocket import RSocket, BaseRequestHandler
from rsocket.payload import Payload
from rxbp.acknowledgement.stopack import StopAck
from rxbp.observer import Observer
from rxbp.schedulers.threadpoolscheduler import ThreadPoolScheduler


def handler(o: Observer, _):
    for i in range(1, 10):
        time.sleep(0.5)
        ack = o.on_next([Payload(b'server data-' + str(i).encode("utf-8"), b'metadata')])
        if isinstance(ack, StopAck):
            print("stopped!")
            break
    o.on_completed()


class Handler(BaseRequestHandler):
    def request_stream(self, payload: Payload):
        data = payload.data.decode('utf-8')
        metadata = payload.metadata.decode('utf-8')
        print("[RS] Received from client: {}, {}".format(data, metadata))

        pool = ThreadPoolScheduler("publisher")

        if metadata == "unlimit":
            publisher = rxbp.interval(1).pipe(
                rxbp.op.map(lambda v: Payload(b'map client2 index-' + str(v).encode('utf-8'), b'metadata')),
                rxbp.op.do_action(
                    on_next=lambda v: print("sending " + v.data.decode('utf-8')),
                    on_error=lambda v: print("sending error: ", str(v)),
                    on_completed=lambda: print("completed!")
                ),
                rxbp.op.subscribe_on(pool),
            )
        else:
            publisher = rxbp.create(handler).pipe(
                rxbp.op.do_action(
                    on_next=lambda v: print("sending " + v.data.decode('utf-8')),
                    on_error=lambda v: print("sending error: ", str(v)),
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
