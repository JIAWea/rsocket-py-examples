import asyncio
import json
import random
import threading
import time

import rxbp
from reactivestreams import Publisher
from rsocket import RSocket, BaseRequestHandler
from rsocket.payload import Payload
from rxbp.acknowledgement.stopack import StopAck
from rxbp.observer import Observer
from rxbp.schedulers.threadpoolschedulerdispose import RXThreadPoolScheduler
from rxbp.typing import ElementType


class ChannelSubscriber:
    def __init__(self):
        self.id = ""
        self.on_next_count = 0
        self.is_cancel = False

    def on_next(self, elem: ElementType):
        for payload in elem:
            print("payload: ", payload)
            str_data = payload.data.decode("utf-8")
            self.id = str_data

        self.on_next_count += 1

        cancel = True if random.randint(1, 201) == 100 else False
        if cancel:
            self.is_cancel = True
            self.subscription.cancel()

        if self.is_cancel:
            print("[Cancel] id: {}, on_next_count: {}".format(self.id, self.on_next_count))

    def on_error(self, exc):
        print("[Exception] id: {}, on_next_count: {}".format(self.id, self.on_next_count))

    def on_completed(self):
        print("[Completed] id: {}, on_next_count: {}".format(self.id, self.on_next_count))

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        self.subscription.request(1000)


class Handler(BaseRequestHandler):
    def request_fire_and_forget(self, payload: Payload):
        data = payload.data.decode('utf-8')
        metadata = payload.metadata.decode('utf-8')
        print("[FNF] Received from client: {}, {}".format(data, metadata))
        res = {
            "id": data,
            "clientCount": 0,
            "serverCount": 1,
            "isCanceled": False,
            "error": None,
            "requestNList": None
        }

        async def request(bytes_res):
            self.socket.fire_and_forget(Payload(bytes_res, b'metadata'))

        res = json.dumps(res)
        asyncio.run_coroutine_threadsafe(request(res.encode('utf-8')), loop)

    def request_response(self, payload: Payload) -> asyncio.Future:
        data = payload.data.decode('utf-8')
        metadata = payload.metadata.decode('utf-8')
        print("[RR] Received from client: {}, {}".format(data, metadata))

        future = loop.create_future()

        res = {
            "id": data,
            "clientCount": 0,
            "serverCount": 0,
            "isCanceled": False,
            "error": None,
            "requestNList": None
        }

        is_error = True if random.randint(0, 9) < 3 else False
        if is_error:
            err_msg = "random error, data: {}".format(data)
            res["error"] = err_msg
            future.set_exception(ValueError(err_msg))
        else:
            res["serverCount"] = 1
            future.set_result(Payload(b"server " + payload.data, b"server " + payload.metadata))

        async def request(bytes_res):
            self.socket.fire_and_forget(Payload(bytes_res, b'metadata'))

        res = json.dumps(res)
        asyncio.run_coroutine_threadsafe(request(res.encode('utf-8')), loop)
        return future

    def request_stream(self, payload: Payload):
        data = payload.data.decode('utf-8')
        metadata = payload.metadata.decode('utf-8')
        print("[RS] Received from client: {}, {}".format(data, metadata))

        res = {
            "id": data,
            "clientCount": 0,
            "serverCount": 0,
            "isCanceled": False,
            "error": None,
            "requestNList": None
        }

        async def request(bytes_data):
            self.socket.fire_and_forget(Payload(bytes_data, b'metadata'))

        on_next_count = 0

        def on_next_counter(_):
            nonlocal on_next_count
            on_next_count += 1

        def on_completed():
            res["serverCount"] = on_next_count
            str_res = json.dumps(res)
            asyncio.run_coroutine_threadsafe(request(str_res.encode('utf-8')), loop)

        def on_error(exc):
            res["serverCount"] = on_next_count
            res["error"] = str(exc)
            str_res = json.dumps(res)
            asyncio.run_coroutine_threadsafe(request(str_res.encode('utf-8')), loop)

        def on_disposed():
            res["serverCount"] = on_next_count
            res["isCanceled"] = True
            str_res = json.dumps(res)
            asyncio.run_coroutine_threadsafe(request(str_res.encode('utf-8')), loop)

        def handler(o: Observer, _):
            for i in range(1, 201):
                time.sleep(0.1)

                ack = o.on_next([Payload(payload.data, b'metadata')])
                if isinstance(ack, StopAck):
                    break

                is_error = True if random.randint(1, 201) == 200 else False
                if is_error:
                    o.on_error(ValueError("random error, data from client: {}".format(data)))

            o.on_completed()

        return rxbp.create(handler).pipe(
            rxbp.op.do_action(
                on_next=on_next_counter,
                on_completed=on_completed,
                on_error=on_error,
                on_disposed=on_disposed
            ),
            rxbp.op.subscribe_on(pool),
        )

    def request_channel(self, inputs: Publisher):
        printed = False

        res = {
            "id": "",
            "clientCount": 0,
            "serverCount": 0,
            "isCanceled": False,
            "error": None,
            "requestNList": None
        }

        class ChannelSubscriber:
            def __init__(self):
                self.on_next_count = 0
                self.is_cancel = False

            def on_next(self, elem: ElementType):
                for payload in elem:
                    data = payload.data.decode("utf-8")
                    if res["id"] == "":
                        res["id"] = data

                    nonlocal printed
                    if not printed:
                        print("########## request channel {} #############".format(data))
                        printed = True

                self.on_next_count += 1

                cancel = True if random.randint(1, 201) == 100 else False
                if cancel:
                    self.is_cancel = True
                    self.subscription.cancel()

                if self.is_cancel:
                    # print("[Cancel] id: {}, on_next_count: {}".format(
                    #     res["id"], self.on_next_count))
                    pass

            def on_error(self, exc):
                # print("[Exception] id: {}, on_next_count: {}".format(
                #     res["id"], self.on_next_count))
                pass

            def on_completed(self):
                # print("[Completed] id: {}, on_next_count: {}".format(
                #     res["id"], self.on_next_count))
                pass

            def on_subscribe(self, subscription):
                # noinspection PyAttributeOutsideInit
                self.subscription = subscription
                self.subscription.request(1000)

        inputs.subscribe(ChannelSubscriber())

        async def request(bytes_data):
            self.socket.fire_and_forget(Payload(bytes_data, b'metadata'))

        on_next_count = 0

        def on_next_counter(_):
            nonlocal on_next_count
            on_next_count += 1

        def on_completed():
            res["treading"] = threading.current_thread().name
            res["serverCount"] = on_next_count
            str_res = json.dumps(res)
            asyncio.run_coroutine_threadsafe(request(str_res.encode('utf-8')), loop)

        def on_error(exc):
            res["treading"] = threading.current_thread().name
            res["serverCount"] = on_next_count
            res["error"] = str(exc)
            str_res = json.dumps(res)
            asyncio.run_coroutine_threadsafe(request(str_res.encode('utf-8')), loop)

        def on_disposed():
            res["treading"] = threading.current_thread().name
            res["serverCount"] = on_next_count
            res["isCanceled"] = True
            str_res = json.dumps(res)
            asyncio.run_coroutine_threadsafe(request(str_res.encode('utf-8')), loop)

        def handler(o: Observer, _):
            for i in range(1, 200):
                time.sleep(0.1)
                is_error = True if random.randint(1, 201) == 200 else False
                if is_error:
                    o.on_error(ValueError("random error"))
                next_data = res["id"]
                ack = o.on_next([Payload(next_data.encode('utf-8'), b'')])
                if isinstance(ack, StopAck):
                    break
            o.on_completed()

        return rxbp.create(handler).pipe(
            rxbp.op.do_action(
                on_next=on_next_counter,
                on_completed=on_completed,
                on_error=on_error,
                on_disposed=on_disposed
            ),
            rxbp.op.subscribe_on(pool),
        )


def session(reader, writer):
    RSocket(reader, writer, handler_factory=Handler)


if __name__ == '__main__':
    pool = RXThreadPoolScheduler()

    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    service = loop.run_until_complete(asyncio.start_server(
        session, 'localhost', 9898))
    try:
        loop.run_forever()
    finally:
        service.close()
        loop.close()
