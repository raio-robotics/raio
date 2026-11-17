import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task


class Text:
    """Sends a number as decimal text."""

    def serialize(self, value: int) -> bytes:
        return str(value).encode()

    def deserialize(self, data: bytes) -> int:
        return int(data)


class Producer(Node):
    out = OutputPort[int]()

    @task
    async def produce(self):
        await raio.wait(1 * MS)
        self.out.set(42)


class Sink(Node):
    inp = InputPort[int]()

    @reaction(inp)
    def show(self):
        print(f"sink received {self.inp.value}")


producer, sink = Producer(), Sink()
# The connection uses its own serializer in place of pickle.
raio.connect(producer.out, sink.inp, serializer=Text())
raio.run(producer, sink)
