import asyncio

from rsocket import Payload
from rsocket import RSocket


async def session(reader, writer):
    socket = RSocket(reader, writer, server=False)
    payload = Payload(b'The quick brown fox', b'meta')

    socket.fire_and_forget(payload)

    result = await socket.request_response(payload)
    print('[RR] Response from server: {}, {}'.format(
        result.data.decode('utf-8'),
        result.metadata.decode('utf-8'),
    ))

    await asyncio.sleep(10)
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
