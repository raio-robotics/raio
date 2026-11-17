import os
import signal
import subprocess
import sys
from pathlib import Path

EXAMPLE = Path(__file__).parent.parent / "examples" / "interrupt.py"

STUBBORN = """
import time

import raio
from raio import Node, task


class Stubborn(Node):
    @task
    async def main(self):
        print("started")
        await raio.wait(raio.HOUR)

    def shutdown(self):
        print("shutting down")
        time.sleep(3600)


raio.run(Stubborn())
"""


def start(script):
    """Start an app in a process group of its own, as it has in a terminal."""
    command = [sys.executable, str(script)]
    pipe = subprocess.PIPE
    return subprocess.Popen(command, stdout=pipe, stderr=pipe, text=True, process_group=0)


def press_ctrl_c(app):
    """Send what ^C in a terminal sends: an interrupt to the launcher and to every node."""
    os.killpg(app.pid, signal.SIGINT)


def test_ctrl_c_makes_every_node_run_its_shutdown_method_at_once():
    app = start(EXAMPLE)
    assert app.stdout.readline() == "sink got 0 at 100ms\n"
    press_ctrl_c(app)
    printed, errors = app.communicate(timeout=30)
    # At most the tick that was under way still reaches the sink.
    rest = sorted(line for line in printed.splitlines() if not line.startswith("sink got"))
    assert rest == ["sink shut down", "ticker shut down"]
    # The launcher ends as any Python program does on ^C, once its nodes have ended.
    assert errors.rstrip().endswith("KeyboardInterrupt")
    assert app.returncode == -signal.SIGINT


def test_a_second_ctrl_c_ends_nodes_that_are_still_shutting_down(tmp_path):
    script = tmp_path / "stubborn.py"
    script.write_text(STUBBORN)
    app = start(script)
    assert app.stdout.readline() == "started\n"
    press_ctrl_c(app)
    assert app.stdout.readline() == "shutting down\n"
    press_ctrl_c(app)
    # The pipe closes only once the node's process is gone.
    assert app.communicate(timeout=30)[0] == ""
    assert app.returncode == -signal.SIGINT
