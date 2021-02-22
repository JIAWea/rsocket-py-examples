import asyncio
import json
import random
import time

import rxbp
from rsocket import Payload, BaseRequestHandler
from rsocket import RSocket
from rxbp.acknowledgement.stopack import StopAck
from rxbp.observer import Observer
from rxbp.schedulers.threadpoolscheduler import ThreadPoolScheduler
from rxbp.schedulers.threadpoolschedulerdispose import RXThreadPoolScheduler
from rxbp.typing import ElementType

received_response = {}
fire_response = {}

received_stream = {}
fire_stream = {}

received_channel = {}
fire_channel = {}


def compare_in_received_response(key):
    if key in received_response:
        if fire_response[key]["serverCount"] == received_response[key]:
            print("[FNF success] id: {}, except: {}, received: {}".format(
                key, fire_response[key], received_response[key]))
        else:
            print("[FNF error] id: {}, except: {}, received: {}".format(
                key, fire_response[key], received_response[key]))


def compare_in_received_stream(key):
    if key in received_stream:
        if fire_stream[key]["serverCount"] == received_stream[key]:
            print("[FNF success] id: {}, except: {}, received: {}".format(
                key, fire_stream[key], received_stream[key]))
        else:
            print("[FNF error] id: {}, except: {}, received: {}".format(
                key, fire_stream[key], received_stream[key]))


def compare_in_received_channel(key):
    if key in received_channel:
        if fire_channel[key]["serverCount"] == received_channel[key]:
            print("[FNF success] id: {}, except: {}, received: {}".format(
                key, fire_channel[key], received_channel[key]))
        else:
            print("[FNF error] id: {}, except: {}, received: {}".format(
                key, fire_channel[key], received_channel[key]))


def compare_in_fire_response(key):
    if key in fire_response:
        if received_response[key] != fire_response[key]["serverCount"]:
            return False
        print("[success] id: {}, except: {}, received: {}".format(
            key, fire_response[key], received_response[key]))
    return True


def compare_in_fire(key):
    if key in fire_stream:
        if received_stream[key] != fire_stream[key]["serverCount"]:
            return False
        print("[success] id: {}, except: {}, received: {}".format(
            key, fire_stream[key], received_stream[key]))
    return True


def compare_in_fire_channel(key):
    if key in fire_channel:
        if received_channel[key] != fire_channel[key]["serverCount"]:
            return False
        print("[success] id: {}, except: {}, received: {}".format(
            key, fire_channel[key], received_channel[key]))
    return True


class StreamSubscriber:
    def __init__(self):
        self.id = ""
        self.on_next_count = 0
        self.is_cancel = False

    def on_next(self, elem: ElementType):
        for payload in elem:
            str_data = payload.data.decode("utf-8")
            self.id = str_data
        self.on_next_count += 1

        cancel = True if random.randint(1, 201) == 200 else False
        if cancel:
            self.is_cancel = True
            self.subscription.cancel()

        if self.is_cancel:
            key = self.id.split(":", 1)[0]
            received_stream[key] = self.on_next_count

            if not compare_in_fire(key):
                print("[error in cancel] except: {}, received: {}".format(
                    fire_stream[key], received_stream[key]))

    def on_error(self, exc):
        key = self.id.split(":", 1)[0]
        received_stream[key] = self.on_next_count

        if not compare_in_fire(key):
            print("[error in error] except: {}, received: {}".format(
                fire_stream[key], received_stream[key]))

    def on_completed(self):
        key = self.id.split(":", 1)[0]
        received_stream[key] = self.on_next_count

        if not compare_in_fire(key):
            print("[error in completed] except: {}, received: {}".format(
                fire_stream[key], received_stream[key]))

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        self.subscription.request(1000)


class ChannelSubscriber:
    def __init__(self):
        self.id = ""
        self.on_next_count = 0
        self.is_cancel = False

    def on_next(self, elem: ElementType):
        for payload in elem:
            self.id = payload.data.decode("utf-8")
        self.on_next_count += 1

        cancel = False
        if self.on_next_count == 10:
            cancel = True
        # cancel = True if random.randint(1, 201) == 100 else False

        if cancel:
            self.is_cancel = True
            self.subscription.cancel()

        if self.is_cancel:
            key = self.id.split(":", 1)[0]
            received_channel[key] = self.on_next_count

            # print("[ChannelSubscriber cancel] id: {}, on_next_count: {}".format(key, self.on_next_count))

            # if not compare_in_fire_channel(key):
            #     print("[error in cancel] except: {}, received: {}".format(
            #         fire_channel[key], received_channel[key]))
            pass

    def on_error(self, exc):
        key = self.id.split(":", 1)[0]
        received_channel[key] = self.on_next_count

        # print("[ChannelSubscriber error] id: {}, on_next_count: {}".format(key, self.on_next_count))

        # if not compare_in_fire_channel(key):
        #     print("[error in cancel] except: {}, received: {}".format(
        #         fire_channel[key], received_channel[key]))
        pass

    def on_completed(self):
        key = self.id.split(":", 1)[0]
        received_channel[key] = self.on_next_count

        # print("[ChannelSubscriber completed] id: {}, on_next_count: {}".format(key, self.on_next_count))

        # if not compare_in_fire_channel(key):
        #     print("[error in cancel] except: {}, received: {}".format(
        #         fire_channel[key], received_channel[key]))
        pass

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription
        self.subscription.request(1000)


class Handler(BaseRequestHandler):
    def request_fire_and_forget(self, payload: Payload):
        str_data = payload.data.decode('utf-8')
        data = json.loads(str_data)

        # *************************** fire ***************************
        # print("[FNF] data: {}".format(data))
        # *************************** fire ***************************

        # *************************** response ***************************
        # key = data["id"]
        # fire_response[key] = data
        # compare_in_received_response(key)
        # *************************** response ***************************

        # *************************** stream ***************************
        # key = data["id"]
        # fire_stream[key] = data
        # compare_in_received_stream(key)
        # *************************** stream ***************************

        # *************************** channel ***************************
        # key = data["id"]
        # fire_channel[key] = data
        # compare_in_received_channel(key)
        print("fire and forget: ", data)
        # *************************** channel ***************************


async def session(reader, writer):
    socket = RSocket(reader, writer, handler_factory=Handler, server=False)
    payload = Payload(b'The quick brown fox', b'meta')

    # async def fire(i):
    #     data = Payload(str(i).encode('utf-8'), b'metadata')
    #     socket.fire_and_forget(data)
    # tasks = [fire(i) for i in range(1, 101)]
    # await asyncio.gather(*tasks)
    #
    # async def response(i):
    #     data = Payload(str(i).encode('utf-8'), b'metadata')
    #     try:
    #         result = await socket.request_response(data)
    #     except Exception as e:
    #         exc_msg = {"id": str(i), "err_msg": str(e)}
    #         raise RuntimeError(json.dumps(exc_msg))
    #     else:
    #         return result
    #
    # tasks_response = [response(i) for i in range(1, 101)]
    # finish, pending = await asyncio.wait(tasks_response)
    # for task in finish:
    #     exc = task.exception()
    #     if exc:
    #         err_data = json.loads(str(exc))
    #         key = err_data["id"]
    #         received_response[key] = 0
    #     else:
    #         str_data = task.result().data.decode("utf-8")
    #         (key, count) = str_data.split(":", 1)
    #         received_response[key] = int(count)
    #
    #     if not compare_in_fire_response(key):
    #         print("[error] id: {}, except: {}, received: {}".format(
    #             key, fire_response[key], received_response[key]
    #         ))

    # async def stream(i):
    #     data = Payload(str(i).encode('utf-8'), b'metadata')
    #     return socket.request_stream(data).subscribe(StreamSubscriber())
    # tasks_stream = [stream(i) for i in range(1, 201)]
    # done, pending = await asyncio.wait(tasks_stream)
    # for task in done:
    #     print("task: ", task.result())

    async def channel(n):
        def handler(o: Observer, _):
            for _ in range(1, 100):
                time.sleep(0.1)
                is_error = True if random.randint(1, 201) == 200 else False
                if is_error:
                    o.on_error(ValueError("random error"))
                ack = o.on_next([Payload(str(n).encode("utf-8"), b'')])
                if isinstance(ack, StopAck):
                    break
            o.on_completed()

        socket.request_channel(
            rxbp.create(handler).pipe(
                rxbp.op.subscribe_on(pool)
            )
        ).subscribe(ChannelSubscriber())

    tasks_channel = [channel(i) for i in range(1, 101)]
    await asyncio.wait(tasks_channel)

    await asyncio.sleep(1000)
    await socket.close()
    return payload


if __name__ == '__main__':
    pool = RXThreadPoolScheduler()
    # pool = ThreadPoolScheduler("")

    loop = asyncio.get_event_loop()
    # loop.set_debug(True)
    try:
        connection = loop.run_until_complete(asyncio.open_connection(
            'localhost', 9898))
        loop.run_until_complete(session(*connection))
    finally:
        loop.close()
