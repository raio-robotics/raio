import raio
from raio import MS, InputPort, Node, OutputPort, task


class Producer(Node):
    out = OutputPort[str]()

    @task
    async def produce(self):
        await raio.sleep(1 * MS)
        self.out.set("hello world")


class Sink(Node):
    inp = InputPort[str]()

    @task
    async def show(self):
        async for value in self.inp:
            print(f"sink received {value!r}")


producer, sink = Producer(), Sink()
raio.connect(producer.out, sink.inp)
raio.run(producer, sink)
