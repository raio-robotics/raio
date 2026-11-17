import sys
from pathlib import Path

import pytest
import raio
from raio import MS, Node, OutputPort, RustNode, task
from test_app import Printer, lines

sys.path.append(str(Path(__file__).parent.parent / "examples"))
from scale import Scale  # noqa: E402  The Python interface of raio-rs's `scale` example.


class Source(Node):
    out = OutputPort[object]()

    def __init__(self, *values):
        self.values = values

    @task
    async def send(self):
        for value in self.values:
            await raio.sleep(1 * MS)
            self.out.set(value)


def test_python_and_rust_nodes_exchange_values_as_messagepack(rust_nodes, capfd):
    source = Source({"name": "a", "value": 1.5}, {"value": 2, "name": "b"})
    double, tenfold, printer = Scale(2.0), Scale(10.0), Printer()
    raio.connect(source.out, double.inp)
    raio.connect(double.out, tenfold.inp)  # Two Rust nodes use what their programs set.
    raio.connect(tenfold.out, printer.inp)
    raio.run(source, double, tenfold, printer)
    assert lines(capfd) == [
        "printer got {'name': 'a', 'value': 30.0} at 1ms",
        "printer got {'name': 'b', 'value': 40.0} at 2ms",
    ]


class Text:
    """A serializer that only Python has."""

    def serialize(self, value):
        return repr(value).encode()

    def deserialize(self, data):
        return data.decode()


def test_a_serializer_that_a_rust_node_lacks_is_refused(rust_nodes, capfd):
    source, scale = Source({"name": "a", "value": 1.5}), Scale(2.0)
    raio.connect(source.out, scale.inp, serializer=Text())
    with pytest.raises(ValueError, match='input port 0 of node 1 uses the serializer "msgpack"'):
        raio.run(source, scale)


def test_a_connection_between_rust_nodes_takes_no_serializer():
    first, second = Scale(2.0), Scale(10.0)
    raio.connect(first.out, second.inp, serializer=raio.MessagePack())
    with pytest.raises(ValueError, match="Rust nodes choose their own serializer"):
        raio.run(first, second)


def test_python_nodes_can_use_messagepack_too(capfd):
    source, printer = Source((1, "two")), Printer()
    raio.connect(source.out, printer.inp, serializer=raio.MessagePack())
    raio.run(source, printer)
    # MessagePack has no tuples, so the value arrives as a list.
    assert lines(capfd) == ["printer got [1, 'two'] at 1ms"]


def test_a_rust_node_needs_a_way_to_start_its_program():
    class Bare(RustNode):
        pass

    with pytest.raises(NotImplementedError, match="Bare needs a start method"):
        raio.run(Bare())
