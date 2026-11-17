import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task
from scale import Scale


class Thermometer(Node):
    out = OutputPort[dict]()

    @task
    async def measure(self):
        for value in (20.5, 21.0):
            await raio.sleep(1 * MS)
            self.out.set({"name": "celsius", "value": value})


class Display(Node):
    inp = InputPort[dict]()

    @reaction(inp)
    def show(self):
        print(f"{self.inp.value} at {self.inp.time}")


# The middle node is written in Rust; its connections to the Python nodes use MessagePack.
thermometer, scale, display = Thermometer(), Scale(10.0), Display()
raio.connect(thermometer.out, scale.inp)
raio.connect(scale.out, display.inp)
raio.run(thermometer, scale, display)
