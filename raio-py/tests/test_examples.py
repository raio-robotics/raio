import concurrent.futures
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).parent.parent / "examples"
NAMES = ["action", "hello", "join", "mixed", "ping_pong", "reaction", "same_timestamp"]
NAMES += ["select_first", "serializer", "shutdown", "sync"]


def run_example(name):
    """Run an example script to its end."""
    command = [sys.executable, str(EXAMPLES / f"{name}.py")]
    keys = "raio" if name == "action" else ""  # The action example reads its keys from stdin.
    return subprocess.run(command, input=keys, capture_output=True, text=True, timeout=60)


@pytest.fixture(scope="module")
def finished(rust_nodes):
    """Run every example at once, since each paces itself on the wall clock."""
    with concurrent.futures.ThreadPoolExecutor(len(NAMES)) as pool:
        return dict(zip(NAMES, pool.map(run_example, NAMES)))


def printed(finished, name):
    """Return the lines that an example printed."""
    result = finished[name]
    assert result.returncode == 0, result.stderr
    return result.stdout.splitlines()


def test_action(finished):
    assert printed(finished, "action") == [f"echo {key} (physical: True)" for key in "raio"]


def test_hello(finished):
    assert printed(finished, "hello") == ["sink received 'hello world'"]


def test_join(finished):
    assert sorted(printed(finished, "join")) == sorted(
        [f"consume {i} at {i + 1}s" for i in range(4)]
        + [f"produce {i} at {700 * (i + 1)}ms" for i in range(4)]
    )


def test_mixed(finished):
    assert printed(finished, "mixed") == [
        "{'name': 'celsius', 'value': 205.0} at 1ms",
        "{'name': 'celsius', 'value': 210.0} at 2ms",
    ]


def test_ping_pong(finished):
    # Each trip around the loop takes the delay of the connection back.
    assert printed(finished, "ping_pong") == [f"ping got {i} at {i}1ms" for i in (1, 2, 3)]


def test_reaction(finished):
    assert printed(finished, "reaction") == [
        "900ms: left 0 at 900ms, right none",
        "1s: left 0 at 900ms, right 0 at 1s",
        "1800ms: left 1 at 1800ms, right 0 at 1s",
        "2s: left 1 at 1800ms, right 1 at 2s",
        "2700ms: left 2 at 2700ms, right 1 at 2s",
        "3600ms: left 3 at 3600ms, right 1 at 2s",
        "4500ms: left 4 at 4500ms, right 1 at 2s",
        "5400ms: left 5 at 5400ms, right 1 at 2s",
    ]


def test_same_timestamp(finished):
    assert printed(finished, "same_timestamp") == ["worker sets 7 at 700ms", "sink got 7 at 700ms"]


def test_select_first(finished):
    assert printed(finished, "select_first") == [
        "900ms: left 0 at 900ms, right none",
        "1s: left 0 at 900ms, right 0 at 1s",
        "1800ms: left 1 at 1800ms, right 0 at 1s",
        "2s: left 1 at 1800ms, right 1 at 2s",
    ]


def test_serializer(finished):
    assert printed(finished, "serializer") == ["sink received 42"]


def test_shutdown(finished):
    assert sorted(printed(finished, "shutdown")) == [
        "sink got 0 at 1ms",
        "sink got 1 at 2ms",
        "sink got 2 at 3ms",
        "sink got 3 at 4ms",
        "sink shut down at (4ms, 1)",
        "ticker shut down at (4ms, 1)",
    ]


def test_sync(finished):
    assert printed(finished, "sync") == [
        "1s: left 0 and right 0 at 1s",
        "2s: left 1 and right 1 at 2s",
        "3s: left 2 and right 2 at 3s",
    ]
