---
sidebar_position: 1
---

# Quick Start

## Install

Raio runs on Linux and macOS, with Python 3.9 or later:

```bash
pip install raio-py
```

To use it from Rust:

```bash
cargo add raio-rs
```

## Hello World

```python title="hello.py"
import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task

# A node runs in its own process.
class Sender(Node):
    # Nodes communicate through ports.
    # An OutputPort can send values to InputPorts.
    out = OutputPort[str]()

    # When a node starts, its tasks are executed.
    @task
    async def send(self):
        await raio.wait(1 * MS)  # one millisecond of logical time
        self.out.set("hello world")  # send a value through the output port

class Listener(Node):
    # InputPorts receive values. [str] is a type annotation.
    inp = InputPort[str]()

    # A reaction runs each time its port receives a value.
    @reaction(inp)
    def show(self):
        print(self.inp.value)  # read input port values with .value

sender, listener = Sender(), Listener()
raio.connect(sender.out, listener.inp)  # port connections are type checked
raio.run(sender, listener)  # run the nodes until all of them finish
```

```console
$ python hello.py
hello world
```

## Add time

Implement your own timer with `await`:

```python title="timed.py"
import raio
from raio import InputPort, Node, OutputPort, S, reaction, task

class TimerSender(Node):
    out = OutputPort[str]()

    @task
    async def send(self):
        for count in range(3):
            now = await raio.wait(1 * S)  # wait one second of logical time
            self.out.set(f"tick {count} at {now}")

# Listener is the same as before.
class Listener(Node):
    inp = InputPort[str]()

    @reaction(inp)
    def show(self):
        print(self.inp.value)

sender, listener = TimerSender(), Listener()
raio.connect(sender.out, listener.inp)
raio.run(sender, listener)
```

```console
$ python timed.py
tick 0 at 1s
tick 1 at 2s
tick 2 at 3s
```

The timestamps are the same in every run.

To skip the three-second wait, for example in a test:

```python
raio.run(sender, listener, fast_forward=True)  # same output, finishes at once
```

## Next steps

- [More examples](https://github.com/raio-robotics/raio) in Python and Rust
- [Slack](https://join.slack.com/t/raio-hq/shared_invite/zt-4h90ce5q9-adb~cElnfDwekfEjcY0BtA) for questions
