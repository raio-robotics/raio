import threading

import pytest
import raio
from raio import MS, Action, Node, reaction, task
from nodes import Ticker


class Feeder(Node):
    numbers = Action[int]()

    @task
    async def main(self):
        threading.Thread(target=self.feed).start()
        last = raio.now()
        async for number in self.numbers:
            later, last = self.numbers.time > last, self.numbers.time
            print(number, raio.is_physical(), later)
        print("closed", self.numbers.closed)

    def feed(self):
        for number in range(3):
            self.numbers.send(number)
        self.numbers.close()


def test_values_sent_from_a_thread_arrive_in_order_as_physical_events(capfd):
    raio.run(Feeder())
    assert capfd.readouterr().out.splitlines() == [
        "0 True True",  # Each value has a physical timestamp after the one before it.
        "1 True True",
        "2 True True",
        "closed True",
    ]


class Waker(Node):
    wake = Action[str]()

    @task
    async def main(self):
        await raio.wait(1 * MS)
        self.wake.send("first")
        self.wake.send("second")
        print(await self.wake, await self.wake, raio.is_physical(), raio.now() > 1 * MS)
        self.wake.close()
        self.wake.close()  # Closing again changes nothing.
        try:
            self.wake.send("late")
        except RuntimeError as error:
            print(error)
        async for _ in self.wake:
            print("never printed")
        print("closed", self.wake.closed)


def test_a_task_can_send_to_an_action_of_its_own_node_until_it_closes_it(capfd):
    raio.run(Waker())
    assert capfd.readouterr().out.splitlines() == [
        "first second True True",
        "Waker.wake is closed",
        "closed True",
    ]


class Reactor(Node):
    wake = Action[int]()

    @task
    async def main(self):
        self.wake.send(1)
        self.wake.send(2)
        self.wake.close()

    @reaction(wake)
    def show(self):
        print(self.wake.value, raio.is_physical())


def test_an_action_triggers_a_reaction(capfd):
    raio.run(Reactor())
    assert capfd.readouterr().out.splitlines() == ["1 True", "2 True"]


def test_an_action_takes_no_connection():
    with pytest.raises(ValueError, match="connect needs an input port of a node instance"):
        raio.connect(Ticker(1 * MS, 1).out, Waker().wake)
