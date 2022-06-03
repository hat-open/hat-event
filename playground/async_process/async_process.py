import sys
import contextlib
import asyncio
import collections
import time


x = 100
p = 1000


def do_work():
    for i in range(1000):
        i += 1


def process1():
    for i in range(x):
        do_work()
        yield i


def process2():
    res = collections.deque()
    for i in range(x):
        do_work()
        res.append(i)
    return res


async def process3():
    res = collections.deque()
    for i in range(x):
        do_work()
        res.append(i)
    return res


async def process4():
    for i in range(x):
        do_work()
        yield i


@contextlib.contextmanager
def duration():
    t1 = time.monotonic()
    yield None
    t2 = time.monotonic()
    print(f"duration is {t2 - t1} s")


def main():

    # for _ in process1():
    #     pass
    # process2()

    print('\nmain process1')
    with duration():
        for _ in range(p):
            for i in process1():
                pass

    print('\nmain process2')
    with duration():
        for _ in range(p):
            for i in process2():
                pass

    asyncio.run(async_main())


async def async_main():
    # for _ in process1():
    #     pass
    # process2()
    # await process3()
    # async for _ in process4():
    #     pass

    print('\nprocess1')
    with duration():
        for _ in range(p):
            for i in process1():
                pass

    print('\nprocess2')
    with duration():
        for _ in range(p):
            for i in process2():
                pass

    print('\nprocess3')
    with duration():
        for _ in range(p):
            res3 = await process3()
            for i in res3:
                pass

    print('\nprocess4')
    with duration():
        for _ in range(p):
            async for _ in process4():
                pass


if __name__ == "__main__":
    sys.exit(main())
