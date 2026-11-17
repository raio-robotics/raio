import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task


class Ticker(Node):
    out = OutputPort[int]()

    @task
    async def tick(self):
        # The ticker runs until the user presses ^C.
        count = 0
        while True:
            await raio.sleep(100 * MS)
            self.out.set(count)
            count += 1

    def shutdown(self):
        print("ticker shut down")


class Sink(Node):
    inp = InputPort[int]()

    @reaction(inp)
    def show(self):
        print(f"sink got {self.inp.value} at {self.inp.time}")

    def shutdown(self):
        print("sink shut down")


ticker, sink = Ticker(), Sink()
raio.connect(ticker.out, sink.inp)
raio.run(ticker, sink)
