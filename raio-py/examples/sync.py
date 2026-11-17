import raio
from raio import InputPort, Node, S, reaction
from ticker import Ticker


class Synchronizer(Node):
    left = InputPort[int]()
    right = InputPort[int]()

    @reaction(left)
    def pair(self):
        if self.right.present:
            left, right = self.left.value, self.right.value
            print(f"{raio.now()}: left {left} and right {right} at {self.left.time}")


fast, slow, sync = Ticker(1 * S, 6), Ticker(1 * S, 3), Synchronizer()
raio.connect(fast.out, sync.left)
raio.connect(slow.out, sync.right)
raio.run(fast, slow, sync)
