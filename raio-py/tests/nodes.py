"""Nodes and helpers that several test files use."""

import raio
from raio import InputPort, Node, OutputPort, Time, task


class Ticker(Node):
    out = OutputPort[int]()

    def __init__(self, period: Time, ticks: int):
        self.period, self.ticks = period, ticks

    @task
    async def tick(self):
        for i in range(self.ticks):
            await raio.wait(self.period)
            self.out.set(i)


class Printer(Node):
    inp = InputPort[int]()

    def __init__(self, name: str = "printer"):
        self.name = name

    @task
    async def show(self):
        async for value in self.inp:
            print(f"{self.name} got {value} at {raio.now()}")


def lines(capfd):
    """Return the sorted output of the nodes, which print from their own processes."""
    return sorted(capfd.readouterr().out.splitlines())
