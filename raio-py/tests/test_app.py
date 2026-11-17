import threading

import pytest
import raio
from raio import MS, Closed, InputPort, Node, OutputPort, Time, task


class Ticker(Node):
    out = OutputPort[int]()

    def __init__(self, period: Time, ticks: int):
        self.period, self.ticks = period, ticks

    @task
    async def tick(self):
        for i in range(self.ticks):
            await raio.sleep(self.period)
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


def test_values_reach_another_process_at_their_tags(capfd):
    ticker, printer = Ticker(1 * MS, 3), Printer()
    raio.connect(ticker.out, printer.inp)
    raio.run(ticker, printer)
    assert lines(capfd) == [f"printer got {i} at {i + 1}ms" for i in range(3)]


def test_a_delay_shifts_the_tags_of_a_connection(capfd):
    ticker, printer = Ticker(1 * MS, 2), Printer()
    raio.connect(ticker.out, printer.inp, delay=10 * MS)
    raio.run(ticker, printer)
    assert lines(capfd) == ["printer got 0 at 11ms", "printer got 1 at 12ms"]


def test_one_output_feeds_several_inputs(capfd):
    ticker, near, far = Ticker(1 * MS, 1), Printer("near"), Printer("far")
    raio.connect(ticker.out, near.inp)
    raio.connect(ticker.out, far.inp, delay=5 * MS)
    raio.run(ticker, near, far)
    assert lines(capfd) == ["far got 0 at 6ms", "near got 0 at 1ms"]


def test_fast_forward_does_not_wait_for_the_wall_clock(capfd):
    ticker, printer = Ticker(raio.HOUR, 2), Printer()
    raio.connect(ticker.out, printer.inp)
    raio.run(ticker, printer, fast_forward=True)
    assert lines(capfd) == ["printer got 0 at 1h", "printer got 1 at 2h"]


class Reader(Node):
    inp = InputPort[int]()

    @task
    async def read(self):
        assert self.inp.value is None and self.inp.time == Time.NEVER
        first = await self.inp
        print(f"first {first}, value {self.inp.value}, time {self.inp.time}")
        print(f"present {self.inp.present}, closed {self.inp.closed}")
        await raio.sleep(500 * raio.US)
        print(f"later: value {self.inp.value}, present {self.inp.present}")
        print(f"second {await self.inp} at {raio.now()}")
        try:
            await self.inp
        except Closed as closed:
            print(f"closed: {closed}, {self.inp.closed}")
        await self.inp  # A second Closed ends the task without failing the node.
        print("never printed")


def test_an_input_keeps_its_latest_value_and_raises_closed_at_its_end(capfd):
    ticker, reader = Ticker(1 * MS, 2), Reader()
    raio.connect(ticker.out, reader.inp)
    raio.run(ticker, reader)
    assert capfd.readouterr().out.splitlines() == [
        "first 0, value 0, time 1ms",
        "present True, closed False",
        "later: value 0, present False",
        "second 1 at 2ms",
        "closed: Reader.inp is closed, True",
    ]


def test_a_node_keeps_the_state_its_constructor_set(capfd):
    class Greeter(Node):
        def __init__(self, greeting):
            self.greeting = greeting

        @task
        async def greet(self):
            print(f"{self.greeting} at {raio.now()}")

    raio.run(Greeter({"hello": [1, 2]}))
    assert capfd.readouterr().out == "{'hello': [1, 2]} at 0ns\n"


def test_tasks_run_in_the_order_they_are_declared(capfd):
    # A class defined here is sent by value, which sorts its attributes by name.
    class Pair(Node):
        @task
        async def zebra(self):
            print("zebra starts")
            await raio.sleep(1 * MS)
            print("zebra resumes")

        @task
        async def apple(self):
            print("apple starts")
            await raio.sleep(1 * MS)
            print("apple resumes")

    raio.run(Pair())
    order = ["zebra starts", "apple starts", "zebra resumes", "apple resumes"]
    assert capfd.readouterr().out.splitlines() == order


def test_an_error_in_a_task_fails_the_app(capfd):
    class Faulty(Node):
        @task
        async def fail(self):
            await raio.sleep(1 * MS)
            raise ValueError("broken")

    with pytest.raises(RuntimeError, match=r"node 0 \(Faulty\) exited with status 1"):
        raio.run(Faulty(), Ticker(1 * MS, 1_000_000))
    assert "ValueError: broken" in capfd.readouterr().err


def test_an_output_cannot_be_set_at_the_start_tag(capfd):
    class Eager(Node):
        out = OutputPort[int]()

        @task
        async def set(self):
            self.out.set(1)

    with pytest.raises(RuntimeError, match="exited with status 1"):
        raio.run(Eager())
    assert "tag 0 completes at startup, so wait first" in capfd.readouterr().err


def test_an_input_without_a_connection_is_refused(capfd):
    with pytest.raises(ValueError, match="input port 0 of node 0 has no connection"):
        raio.run(Printer())
    capfd.readouterr()


def test_a_connection_needs_both_of_its_nodes():
    ticker, printer = Ticker(1 * MS, 1), Printer()
    raio.connect(ticker.out, printer.inp)
    with pytest.raises(ValueError, match="only one of their nodes is run"):
        raio.run(ticker)


def test_connect_takes_an_output_and_an_input_of_node_instances():
    ticker, printer = Ticker(1 * MS, 1), Printer()
    with pytest.raises(ValueError, match="an output port of a node instance"):
        raio.connect(Ticker.out, printer.inp)
    with pytest.raises(ValueError, match="an input port of a node instance"):
        raio.connect(ticker.out, ticker.out)
    with pytest.raises(ValueError, match="a Time of zero or more nanoseconds"):
        raio.connect(ticker.out, printer.inp, delay=-1 * MS)


def test_a_port_belongs_to_a_node_only_if_its_class_declares_it():
    class Late(Node):
        def __init__(self):
            self.out = OutputPort[int]()

    with pytest.raises(ValueError, match="an output port of a node instance"):
        raio.connect(Late().out, Printer().inp)


def test_a_port_works_only_inside_its_running_node():
    ticker = Ticker(1 * MS, 1)
    with pytest.raises(RuntimeError, match="only by its own node, while that node runs"):
        ticker.out.set(1)
    with pytest.raises(RuntimeError, match="is a declaration"):
        Ticker.out.set(1)
    with pytest.raises(RuntimeError, match="no raio node is running"):
        raio.now()


def test_a_node_that_cannot_be_sent_fails_before_anything_starts():
    class Locked(Node):
        def __init__(self):
            self.lock = threading.Lock()

    with pytest.raises(TypeError, match="pickle"):
        raio.run(Locked())


def _fail_to_load():
    raise OSError("cannot load here")


def test_a_node_that_cannot_be_rebuilt_in_its_process_fails_the_app(capfd):
    class Unloadable(Node):
        def __reduce__(self):
            return _fail_to_load, ()

    # The healthy node waits unwired, so the node that failed is the one reported.
    with pytest.raises(RuntimeError, match=r"node 1 \(Unloadable\) exited with status 1"):
        raio.run(Ticker(1 * MS, 1_000_000), Unloadable())
    errors = capfd.readouterr().err
    assert "OSError: cannot load here" in errors and "panicked" not in errors


def test_each_instance_has_its_own_ports():
    first, second = Ticker(1 * MS, 1), Ticker(1 * MS, 1)
    assert first.out is not second.out and first.out is not Ticker.out
    assert repr(first.out) == "Ticker.out" and repr(Ticker.out) == "<declaration>.out"
