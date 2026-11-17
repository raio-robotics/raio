import subprocess
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def rust_nodes():
    """Build the Rust nodes that the mixed-language apps run."""
    command = ["cargo", "build", "--quiet", "--package", "raio-rs", "--example", "scale"]
    subprocess.run(command, cwd=Path(__file__).parents[2], check=True)
