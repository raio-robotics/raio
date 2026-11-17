import raio


def test_add():
    assert raio.add(2, 3) == 5


def test_add_zero():
    assert raio.add(0, 0) == 0
