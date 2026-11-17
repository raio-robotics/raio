import sys
import threading

import raio
from raio import Action, InputPort, Node, OutputPort, reaction, task


class Keyboard(Node):
    keys = Action[str]()
    out = OutputPort[str]()

    @task
    async def forward(self):
        threading.Thread(target=self.read, daemon=True).start()
        async for key in self.keys:
            self.out.set(key)

    def read(self):
        while key := sys.stdin.read(1):
            self.keys.send(key)
        self.keys.close()


class Echo(Node):
    inp = InputPort[str]()

    @reaction(inp)
    def echo(self):
        print(f"echo {self.inp.value} (physical: {raio.is_physical()})")


keyboard, echo = Keyboard(), Echo()
raio.connect(keyboard.out, echo.inp)
raio.run(keyboard, echo)
