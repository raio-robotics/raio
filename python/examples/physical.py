import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task


class Ping(Node):
    inp = InputPort[int]()
    out = OutputPort[int]()

    @task
    async def ping(self):
        count = 0
        for _ in range(3):
            # Ping takes its time: it never waits for what Pong may still send.
            await raio.wait(1 * MS)
            self.out.set(count)
            count = await self.inp
            print(f"ping got {count} (physical: {raio.is_physical()})")


class Pong(Node):
    inp = InputPort[int]()
    out = OutputPort[int]()

    @reaction(inp)
    def pong(self):
        self.out.set(self.inp.value + 1)


ping, pong = Ping(), Pong()
raio.connect(ping.out, pong.inp)
# Ping's clock stamps each reply when it arrives, so this loop needs no delay.
raio.connect(pong.out, ping.inp, physical=True)
raio.run(ping, pong)
