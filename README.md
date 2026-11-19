<!-- markdownlint-disable MD001 MD041 -->
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/raio-robotics/raio/main/docs/assets/logo/raio-lockup-white-rounded.png">
    <img alt="RAIO" src="https://raw.githubusercontent.com/raio-robotics/raio/main/docs/assets/logo/raio-lockup-black-rounded.png" width=40%>
  </picture>
</p>

<h3 align="center">
RAIO: Realtime Asynchronous Deterministic Robotic Middleware
</h3>

<p align="center">
<a href="https://raio-robotics.com"><b>Docs</b></a> | <a href="https://raio-robotics.com/blog"><b>Blog</b></a> | <a href="https://join.slack.com/t/raio-hq/shared_invite/zt-4h90ce5q9-adb~cElnfDwekfEjcY0BtA"><b>Slack</b></a> |
<a href="https://pypi.org/project/raio-py/"><img alt="PyPI version" src="https://img.shields.io/pypi/v/raio-py" align="absmiddle"></a>
<a href="https://crates.io/crates/raio-rs"><img alt="crates.io version" src="https://img.shields.io/crates/v/raio-rs" align="absmiddle"></a>
<a href="https://github.com/raio-robotics/raio/blob/main/LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-blue" align="absmiddle"></a>
</p>

---

Raio is a realtime robotic middleware that is deterministic. Time is treated as a first order primitive, integrated with the `async` / `await` syntax.

## Get Started

Install Raio for Python 3.9 or later:

```bash
pip install raio-py
```

Raio is built with Rust, to add it as a dependency:
```bash
cargo add raio-rs
```

## Hello World

Two nodes, each in its own process: `Sender` sends a value and `Listener` prints it.

```python
import raio
from raio import MS, InputPort, Node, OutputPort, reaction, task

class Sender(Node):
    out = OutputPort[str]()

    @task
    async def send(self):
        await raio.wait(1 * MS)
        self.out.set("hello world")

class Listener(Node):
    inp = InputPort[str]()

    @reaction(inp)
    def show(self):
        print(self.inp.value)

sender, listener = Sender(), Listener()
raio.connect(sender.out, listener.inp)
raio.run(sender, listener)
```

```console
$ python hello.py
hello world
```

## Learn More

The [documentation](https://raio-robotics.com) has a tutorial, the concepts behind Raio, and the
API reference for Python and Rust.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build RAIO from source, run the tests, and make
a release.
