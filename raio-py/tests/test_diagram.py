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
    # A node has its id in its corner and the name of its class; a task has the name of its
    # method, and a port its own.
    shown = ["10ms", "0", "Ticker", "tick", "out", "1", "Printer", "show", "inp"]
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


def test_a_diagram_shows_an_action_inside_its_node_and_a_reaction_among_the_tasks(tmp_path):
    keyboard = Keyboard()
    raio.connect(keyboard.typed, keyboard.echo)
    raio.diagram(tmp_path / "keyboard.svg", keyboard)
    svg = (tmp_path / "keyboard.svg").read_text()
    shown = ["0", "Keyboard", "P", "keys", "read", "show", "echo", "typed"]
    assert texts(svg) == shown
    assert svg.count('class="action"') == 1 and svg.count('class="task"') == 2


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
