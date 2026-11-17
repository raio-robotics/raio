import raio
from raio import MS, InputPort, Node, S, task
from ticker import Ticker


class Worker(Node):
    inp = InputPort[int]()

    @task
    async def consume(self):
        async for value in self.inp:
            print(f"consume {value} at {raio.now()}")

    @task
    async def produce(self):
        for i in range(4):
            now = await raio.wait(700 * MS)
            print(f"produce {i} at {now}")


ticker, worker = Ticker(1 * S, 4), Worker()
raio.connect(ticker.out, worker.inp)
raio.run(ticker, worker)
