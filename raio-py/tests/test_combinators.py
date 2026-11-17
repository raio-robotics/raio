import pytest
import raio
from raio import MS, S, InputPort, Node, task
from test_app import Ticker


def feed(sink, *tickers):
    """Run a sink in fast-forward, fed by tickers on its inputs named a, b, ..."""
    for name, ticker in zip("ab", tickers):
        raio.connect(ticker.out, getattr(sink, name))
    raio.run(*tickers, sink, fast_forward=True)


class Gather(Node):
    a = InputPort[int]()
    b = InputPort[int]()

    @task
    async def main(self):
        first, second = await raio.gather(self.a, self.b)
        print(raio.now(), first, second)
        print(*await raio.gather(raio.sleep(2 * S), self.pause(), raio.sleep(1 * S)))
        print(await raio.gather())

    async def pause(self):
        await raio.sleep(500 * MS)
        return await raio.sleep(500 * MS)


def test_gather_returns_every_result_in_order_once_all_have_finished(capfd):
    feed(Gather(), Ticker(300 * MS, 1), Ticker(1 * S, 1))
    assert capfd.readouterr().out.splitlines() == ["1s 0 0", "3s 2s 2s", "()"]


class Failing(Node):
    @task
    async def main(self):
        try:
            await raio.gather(self.slow(), self.fail())
        except ValueError as error:
            print(f"caught {error} at {raio.now()}")

    async def slow(self):
        try:
            await raio.sleep(1 * raio.HOUR)
        finally:
            print("slow is dropped")

    async def fail(self):
        await raio.sleep(1 * MS)
        raise ValueError("broken")


def test_gather_raises_the_first_error_and_drops_the_others(capfd):
    raio.run(Failing(), fast_forward=True)
    assert capfd.readouterr().out.splitlines() == ["slow is dropped", "caught broken at 1ms"]


class Select(Node):
    a = InputPort[int]()
    b = InputPort[int]()

    @task
    async def main(self):
        print(await raio.select(self.a, self.b))
        print(await raio.select(self.a, self.b))
        match await raio.select(value=self.b, late=raio.sleep(10 * S)):
            case {"value": value}:
                print("value", value, raio.now())
        match await raio.select(value=self.b, late=raio.sleep(250 * MS)):
            case {"late": tag}:
                print("late", tag)
        async for _ in self.a:
            pass
        try:
            await raio.select(self.a, self.b)
        except raio.Closed as closed:
            print("closed:", closed)


def test_select_returns_what_finished_first_and_drops_the_others(capfd):
    feed(Select(), Ticker(500 * MS, 2), Ticker(1 * S, 3))
    assert capfd.readouterr().out.splitlines() == [
        "{0: 0}",
        "{0: 1, 1: 0}",  # Both inputs have a value at 1s.
        "value 1 2s",
        "late 2250ms",
        "closed: Select.a is closed",
    ]


class Timeouts(Node):
    a = InputPort[int]()

    @task
    async def main(self):
        try:
            await raio.timeout(250 * MS, self.a)
        except TimeoutError as expired:
            print(f"{expired} at {raio.now()}")
        try:
            await raio.timeout(500 * MS, self.slow())
        except TimeoutError:
            print(f"slow is dropped: {self.dropped} at {raio.now()}")
        print("value", await raio.timeout(10 * S, self.a), raio.now())
        print("sleep", await raio.sleep(700 * MS))

    async def slow(self):
        try:
            await raio.sleep(1 * raio.HOUR)
        finally:
            self.dropped = True


def test_timeout_raises_once_the_duration_has_passed_and_drops_what_it_awaited(capfd):
    feed(Timeouts(), Ticker(1 * S, 1))
    assert capfd.readouterr().out.splitlines() == [
        "no result within 250ms at 250ms",
        "slow is dropped: True at 750ms",
        "value 0 1s",
        "sleep 1700ms",
    ]


class Spawner(Node):
    a = InputPort[int]()

    @task
    async def main(self):
        doubled = raio.spawn(self.double())
        print("spawned", doubled.done(), raio.now())
        print("joined", await doubled, doubled.done(), raio.now())
        print("again", await doubled)
        raio.spawn(self.late())

    async def double(self):
        return 2 * (await self.a + 1)

    async def late(self):
        print("late", await raio.sleep(1 * S))


def test_a_spawned_task_runs_on_its_own_and_gives_its_result_to_those_that_await_it(capfd):
    feed(Spawner(), Ticker(1 * S, 1))
    assert capfd.readouterr().out.splitlines() == [
        "spawned False 0ns",
        "joined 2 True 1s",
        "again 2",
        "late 2s",  # The node runs on until the task that it spawned last has finished.
    ]


def test_an_error_in_a_spawned_task_fails_the_app(capfd):
    class Faulty(Node):
        @task
        async def main(self):
            await raio.spawn(self.fail())
            print("never printed")

        async def fail(self):
            await raio.sleep(1 * MS)
            raise ValueError("broken")

    with pytest.raises(RuntimeError, match=r"node 0 \(Faulty\) exited with status 1"):
        raio.run(Faulty())
    captured = capfd.readouterr()
    assert "ValueError: broken" in captured.err and "never printed" not in captured.out


def test_a_combinator_takes_awaitables():
    with pytest.raises(TypeError, match="1 is not awaitable"):
        raio.gather(1)
    with pytest.raises(TypeError, match="1 is not awaitable"):
        raio.select(1)
    with pytest.raises(ValueError, match="select needs at least one awaitable"):
        raio.select()
