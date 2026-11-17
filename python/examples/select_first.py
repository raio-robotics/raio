import raio
from raio import MS, InputPort, Node, S, task
from ticker import Ticker


def show(port):
    return "none" if port.value is None else f"{port.value} at {port.time}"


class Merge(Node):
    left = InputPort[int]()
    right = InputPort[int]()

    @task
    async def merge(self):
        while True:
            await raio.select(self.left, self.right)
            print(f"{raio.now()}: left {show(self.left)}, right {show(self.right)}")


fast, slow, merge = Ticker(900 * MS, 6), Ticker(1 * S, 2), Merge()
raio.connect(fast.out, merge.left)
raio.connect(slow.out, merge.right)
raio.run(fast, slow, merge)
