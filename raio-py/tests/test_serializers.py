import raio
from raio import MS
from nodes import Printer, Ticker, lines


class Decimal:
    """Sends a number as decimal text, and marks what it decodes to show that it was used."""

    def serialize(self, value):
        return str(value).encode()

    def deserialize(self, data):
        return f"decimal {data.decode()}"


class Hex:
    """Sends a number as hexadecimal text, and marks what it decodes to show that it was used."""

    def serialize(self, value):
        return hex(value).encode()

    def deserialize(self, data):
        return f"hex {data.decode()}"


def test_each_connection_of_an_output_uses_its_own_serializer(capfd):
    ticker = Ticker(1 * MS, 2)
    plain, decimal, hexadecimal = Printer("plain"), Printer("decimal"), Printer("hex")
    raio.connect(ticker.out, plain.inp)
    raio.connect(ticker.out, decimal.inp, serializer=Decimal())
    raio.connect(ticker.out, hexadecimal.inp, serializer=Hex())
    raio.run(ticker, plain, decimal, hexadecimal)
    assert lines(capfd) == [
        "decimal got decimal 0 at 1ms",
        "decimal got decimal 1 at 2ms",
        "hex got hex 0x0 at 1ms",
        "hex got hex 0x1 at 2ms",
        "plain got 0 at 1ms",
        "plain got 1 at 2ms",
    ]


def test_a_serializer_goes_to_both_nodes_of_its_connection_with_its_state(capfd):
    # A class defined here is sent by value, as one defined in the user's script is.
    class Scaled:
        def __init__(self, factor):
            self.factor = factor

        def serialize(self, value):
            return str(value * self.factor).encode()

        def deserialize(self, data):
            return int(data) + self.factor

    ticker, printer = Ticker(1 * MS, 2), Printer()
    raio.connect(ticker.out, printer.inp, serializer=Scaled(100), delay=1 * MS)
    raio.run(ticker, printer)
    assert lines(capfd) == ["printer got 100 at 2ms", "printer got 200 at 3ms"]
