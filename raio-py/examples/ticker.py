import raio
from raio import Node, OutputPort, Time, task


class Ticker(Node):
    out = OutputPort[int]()

    def __init__(self, period: Time, ticks: int):
        self.period = period
        self.ticks = ticks

    @task
    async def tick(self):
        for i in range(self.ticks):
            await raio.sleep(self.period)
            self.out.set(i)
