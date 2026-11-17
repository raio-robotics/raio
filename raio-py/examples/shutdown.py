import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task


class Ticker(Node):
    out = OutputPort[int]()

    @task
    async def tick(self):
        count = 0
        while True:
            await raio.sleep(1 * MS)
            self.out.set(count)
            if count == 3:
                # Every node of the app shuts down at one tag.
                raio.request_shutdown()
            count += 1

    def shutdown(self):
        print(f"ticker shut down at {raio.now()}")


class Sink(Node):
    inp = InputPort[int]()

    @reaction(inp)
    def show(self):
        print(f"sink got {self.inp.value} at {self.inp.time}")

    def shutdown(self):
        print(f"sink shut down at {raio.now()}")


ticker, sink = Ticker(), Sink()
raio.connect(ticker.out, sink.inp)
raio.run(ticker, sink)
