import subprocess
import sys
from pathlib import Path

EXAMPLES = Path(__file__).parent.parent / "examples"


def run_example(name):
    """Run an example script and return the lines it printed."""
    command = [sys.executable, str(EXAMPLES / f"{name}.py")]
    result = subprocess.run(command, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    return result.stdout.splitlines()


def test_hello():
    assert run_example("hello") == ["sink received 'hello world'"]


def test_join():
    assert sorted(run_example("join")) == sorted(
        [f"consume {i} at {i + 1}s" for i in range(4)]
        + [f"produce {i} at {700 * (i + 1)}ms" for i in range(4)]
    )


def test_select_first():
    assert run_example("select_first") == [
        "900ms: left 0 at 900ms, right none",
        "1s: left 0 at 900ms, right 0 at 1s",
        "1800ms: left 1 at 1800ms, right 0 at 1s",
        "2s: left 1 at 1800ms, right 1 at 2s",
    ]
