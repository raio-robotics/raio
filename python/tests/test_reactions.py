import pytest
import raio
from raio import MS, US, InputPort, Node, OutputPort, reaction
from nodes import Ticker


class Merge(Node):
    left = InputPort[int]()
    right = InputPort[int]()

    @reaction(left, right)
    def show(self):
        present = [name for name in ("left", "right") if getattr(self, name).present]
        print(f"{raio.now()}: {' and '.join(present)}")


def test_a_reaction_runs_once_per_timestamp_at_which_any_trigger_has_a_value(capfd):
    fast, slow, merge = Ticker(1 * MS, 4), Ticker(2 * MS, 2), Merge()
    raio.connect(fast.out, merge.left)
    raio.connect(slow.out, merge.right)
    raio.run(fast, slow, merge)
    assert capfd.readouterr().out.splitlines() == [
        "1ms: left",
        "2ms: left and right",
        "3ms: left",
        "4ms: left and right",
    ]


def test_a_trigger_that_closed_does_not_hide_the_values_of_another(capfd):
    short, long, merge = Ticker(1 * MS, 1), Ticker(1 * MS, 200), Merge()
    raio.connect(short.out, merge.left)
    raio.connect(long.out, merge.right)
    raio.run(short, long, merge, fast_forward=True)
    seen = capfd.readouterr().out.splitlines()
    assert seen == ["1ms: left and right"] + [f"{tick}ms: right" for tick in range(2, 201)]


class Slow(Node):
    inp = InputPort[int]()

    @reaction(inp)
    async def show(self):
        value = self.inp.value
        print(f"{value} at {await raio.wait(500 * US)}")


def test_a_reaction_may_be_async(capfd):
    ticker, slow = Ticker(1 * MS, 2), Slow()
    raio.connect(ticker.out, slow.inp)
    raio.run(ticker, slow)
    assert capfd.readouterr().out.splitlines() == ["0 at 1500us", "1 at 2500us"]


def test_reactions_run_in_the_order_they_are_declared_whichever_port_triggers_them(capfd):
    # A class defined here is sent by value, which sorts its attributes by name.
    class Order(Node):
        a = InputPort[int]()
        b = InputPort[int]()

        @reaction(b)
        def first(self):
            print(f"{raio.now()}: first")

        @reaction(a)
        def second(self):
            print(f"{raio.now()}: second")

        @reaction(a, b)
        def third(self):
            print(f"{raio.now()}: third")

    fast, slow, order = Ticker(1 * MS, 4), Ticker(2 * MS, 2), Order()
    raio.connect(fast.out, order.a)
    raio.connect(slow.out, order.b)
    raio.run(fast, slow, order)
    only_a, both = ["second", "third"], ["first", "second", "third"]
    ran = [f"{ms}ms: {name}" for ms in (1, 2, 3, 4) for name in (only_a if ms % 2 else both)]
    assert capfd.readouterr().out.splitlines() == ran


def test_an_error_in_a_reaction_fails_the_app(capfd):
    class Faulty(Node):
        inp = InputPort[int]()

        @reaction(inp)
        def fail(self):
            raise ValueError("broken")

    ticker, faulty = Ticker(1 * MS, 1_000_000), Faulty()
    raio.connect(ticker.out, faulty.inp)
    with pytest.raises(RuntimeError, match=r"node 2 \(Faulty\) exited with status 1"):
        raio.run(ticker, faulty)
    assert "ValueError: broken" in capfd.readouterr().err


def test_a_reaction_needs_the_input_ports_that_trigger_it():
    with pytest.raises(TypeError, match="input ports or actions that trigger it"):
        reaction()
    with pytest.raises(TypeError, match="input ports or actions that trigger it"):
        reaction(OutputPort[int]())
    with pytest.raises(TypeError, match="input ports or actions that trigger it"):

        @reaction
        def show(self):
            pass
