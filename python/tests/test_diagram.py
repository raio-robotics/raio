import re

import pytest
import raio
from nodes import Printer, Ticker
from raio import MS, Action, InputPort, Node, OutputPort, reaction, task


def texts(svg):
    """Return every text that a diagram shows, in the order in which it is drawn."""
    return re.findall(r">([^<>]+)</text>", svg)


def test_a_diagram_shows_the_nodes_with_their_ports_and_tasks_and_the_connections(tmp_path):
    ticker, printer = Ticker(1 * MS, 3), Printer()
    raio.connect(ticker.out, printer.inp, delay=10 * MS)
    path = tmp_path / "pipeline.svg"
    raio.diagram(path, ticker, printer)
    svg = path.read_text()
    # A node has its id in its corner and the name of its class; a task has its number and the
    # name of its method, and a port its own name.
    shown = ["10ms", "1", "Ticker", "1", "tick", "out", "2", "Printer", "1", "show", "inp"]
    assert texts(svg) == shown
    assert svg.count('class="connection"') == 1 and svg.count('class="port"') == 2


class Keyboard(Node):
    keys = Action[str]()
    typed = OutputPort[str]()
    echo = InputPort[str]()

    @task
    async def read(self):
        pass

    @reaction(keys, echo)
    def show(self):
        pass


def test_a_task_is_a_rectangle_and_a_reaction_has_a_line_from_each_trigger(tmp_path):
    keyboard = Keyboard()
    raio.connect(keyboard.typed, keyboard.echo)
    raio.diagram(tmp_path / "keyboard.svg", keyboard)
    svg = (tmp_path / "keyboard.svg").read_text()
    # The task and the reaction have their numbers, in the order in which the class declares them.
    shown = ["1", "Keyboard", "P", "keys", "1", "read", "2", "show", "echo", "typed"]
    assert texts(svg) == shown
    assert svg.count('class="action"') == 1
    # The task is a rectangle with rounded corners; the reaction is a box with a point, joined
    # to its action and its input port.
    assert svg.count('<rect class="task"') == 1 and svg.count('rx="4"') == 1
    assert svg.count('<polygon class="reaction"') == 1
    assert svg.count('class="trigger"') == 2


def test_a_diagram_draws_a_physical_connection_as_a_dashed_line(tmp_path):
    ticker, near, far = Ticker(1 * MS, 1), Printer("near"), Printer("far")
    raio.connect(ticker.out, near.inp)
    raio.connect(ticker.out, far.inp, physical=True)
    raio.diagram(tmp_path / "app.svg", ticker, near, far)
    svg = (tmp_path / "app.svg").read_text()
    assert svg.count('class="connection"') == 2 and svg.count("stroke-dasharray") == 1


def test_the_same_app_gives_the_same_diagram(tmp_path):
    ticker, printer = Ticker(1 * MS, 3), Printer()
    raio.connect(ticker.out, printer.inp)
    raio.diagram(tmp_path / "app.svg", ticker, printer)
    first = (tmp_path / "app.svg").read_text()
    raio.diagram(str(tmp_path / "app.svg"), ticker, printer)
    assert (tmp_path / "app.svg").read_text() == first


def test_diagram_takes_the_nodes_that_run_takes(tmp_path):
    ticker = Ticker(1 * MS, 1)
    with pytest.raises(ValueError, match="diagram needs at least one node"):
        raio.diagram(tmp_path / "app.svg")
    with pytest.raises(TypeError, match="diagram takes node instances"):
        raio.diagram(tmp_path / "app.svg", Ticker)
    with pytest.raises(ValueError, match="Ticker was given to diagram twice"):
        raio.diagram(tmp_path / "app.svg", ticker, ticker)
    with pytest.raises(OSError):
        raio.diagram(tmp_path / "missing" / "app.svg", ticker)
