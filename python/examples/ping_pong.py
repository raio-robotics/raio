import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task


class Ping(Node):
    inp = InputPort[int]()
    out = OutputPort[int]()

    @task
    async def ping(self):
        # The loop starts here, before the delay of the connection back has passed.
        await raio.wait(1 * MS)
        count = 0
        for _ in range(3):
            self.out.set(count)
            count = await self.inp
            print(f"ping got {count} at {raio.now()}")


class Pong(Node):
    inp = InputPort[int]()
    out = OutputPort[int]()

    @reaction(inp)
    def pong(self):
        self.out.set(self.inp.value + 1)


ping, pong = Ping(), Pong()
raio.connect(ping.out, pong.inp)
# A loop of connections needs a delay.
raio.connect(pong.out, ping.inp, delay=10 * MS)
raio.run(ping, pong)
