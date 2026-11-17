import raio
from raio import MS, InputPort, Node, OutputPort, S, reaction, task
from ticker import Ticker


class Worker(Node):
    inp = InputPort[int]()
    out = OutputPort[int]()

    @task
    async def consume(self):
        async for _ in self.inp:
            pass

    @task
    async def produce(self):
        now = await raio.wait(700 * MS)
        print(f"worker sets 7 at {now}")
        self.out.set(7)


class Sink(Node):
    inp = InputPort[int]()

    @reaction(inp)
    def show(self):
        print(f"sink got {self.inp.value} at {self.inp.time}")


ticker, worker, sink = Ticker(1 * S, 2), Worker(), Sink()
raio.connect(ticker.out, worker.inp)
raio.connect(worker.out, sink.inp)
raio.run(ticker, worker, sink)
