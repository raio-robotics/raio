import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task


class Producer(Node):
    out = OutputPort[str]()

    @task
    async def produce(self):
        await raio.sleep(1 * MS)
        self.out.set("hello world")


class Sink(Node):
    inp = InputPort[str]()

    @reaction(inp)
    def show(self):
        print(f"sink received {self.inp.value!r}")


producer, sink = Producer(), Sink()
raio.connect(producer.out, sink.inp)
raio.run(producer, sink)
