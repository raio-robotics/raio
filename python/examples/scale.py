import subprocess
from pathlib import Path

from raio import InputPort, OutputPort, RustNode

# A package that ships a Rust node carries its program; this one is raio-rs's `scale` example.
PROGRAM = Path(__file__).parents[2] / "target" / "debug" / "examples" / "scale"


class Scale(RustNode):
    """Multiplies the value of each reading by a factor; a reading is a name and a value."""

    inp = InputPort[dict]()
    out = OutputPort[dict]()

    def __init__(self, factor: float):
        self.factor = factor

    def start(self, node_id, coordinator_endpoint):
        # The factor is the program's own argument; the node's place in the app follows it.
        command = [str(PROGRAM), str(self.factor)]
        command += ["--node_id", str(node_id), "--coordinator_endpoint", coordinator_endpoint]
        return subprocess.Popen(command)
