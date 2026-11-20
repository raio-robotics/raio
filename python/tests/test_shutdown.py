from __future__ import annotations

import time

import pytest
import raio
from raio import MS, InputPort, Node, OutputPort, Time, reaction, task
from nodes import lines


class Endless(Node):
    out = OutputPort[int]()

    def __init__(self, stop_at: int, delay: Time | None):
        self.stop_at, self.delay = stop_at, delay

    @task
    async def tick(self):
        count = 0
        while True:
            await raio.wait(1 * MS)
            self.out.set(count)
            if count == self.stop_at and self.delay is None:
                raio.request_shutdown()
            elif count == self.stop_at:
                raio.request_shutdown_after(self.delay)
            count += 1

    def shutdown(self):
        print(f"ticker shut down at {raio.now()}")


class Sink(Node):
    inp = InputPort[int]()

    @reaction(inp)
    def show(self):
        print(f"sink got {self.inp.value} at {self.inp.time}")

    def shutdown(self):
        print(f"sink shut down at {raio.now()}")


def test_a_requested_shutdown_stops_every_node_at_one_timestamp(capfd):
    ticker, sink = Endless(1, None), Sink()
    raio.connect(ticker.out, sink.inp)
    raio.run(ticker, sink)
    assert lines(capfd) == [
        "sink got 0 at 1ms",
        "sink got 1 at 2ms",
        "sink shut down at (2ms, 1)",  # A microstep after the timestamp of the request.
        "ticker shut down at (2ms, 1)",
    ]


def test_a_shutdown_can_be_requested_for_a_later_timestamp(capfd):
    ticker, sink = Endless(0, 3 * MS), Sink()
    raio.connect(ticker.out, sink.inp)
    raio.run(ticker, sink)
    assert lines(capfd) == [
        "sink got 0 at 1ms",
        "sink got 1 at 2ms",
        "sink got 2 at 3ms",  # The tasks run on until the shutdown, where none of them runs.
        "sink shut down at 4ms",
        "ticker shut down at 4ms",
    ]


class Brief(Node):
    @task
    async def main(self):
        print(f"task ends at {await raio.wait(1 * MS)}")
        try:
            raio.request_shutdown_after(Time.ZERO)
        except ValueError as error:
            print(error)

    def shutdown(self):
        print(f"shut down at {raio.now()}")


def test_a_node_that_finishes_on_its_own_shuts_down_at_its_last_timestamp(capfd):
    raio.run(Brief())
    assert capfd.readouterr().out.splitlines() == [
        "task ends at 1ms",
        "request_shutdown_after needs a positive Time, such as 500 * MS",
        "shut down at 1ms",
    ]


def test_a_node_that_fails_makes_the_others_shut_down(capfd):
    class Faulty(Node):
        inp = InputPort[int]()

        @reaction(inp)
        def fail(self):
            raise ValueError("broken")

    ticker, faulty = Endless(-1, None), Faulty()  # This ticker never asks for a shutdown.
    raio.connect(ticker.out, faulty.inp)
    with pytest.raises(RuntimeError, match=r"node 2 \(Faulty\) exited with status 1"):
        raio.run(ticker, faulty)
    assert "ticker shut down at" in capfd.readouterr().out


def test_a_node_that_stops_responding_is_given_up_on(capfd):
    class Stuck(Node):
        @task
        async def main(self):
            await raio.wait(1 * MS)
            time.sleep(3600)  # A task that blocks its thread keeps the node's heartbeat back.

    with pytest.raises(RuntimeError, match=r"node 2 \(Stuck\) stopped responding"):
        raio.run(Endless(-1, None), Stuck())
    assert "ticker shut down at" in capfd.readouterr().out


def test_an_error_in_shutdown_fails_the_app(capfd):
    class Faulty(Node):
        @task
        async def main(self):
            await raio.wait(1 * MS)

        def shutdown(self):
            raise ValueError("broken")

    with pytest.raises(RuntimeError, match=r"node 1 \(Faulty\) exited with status 1"):
        raio.run(Faulty())
    assert "ValueError: broken" in capfd.readouterr().err
