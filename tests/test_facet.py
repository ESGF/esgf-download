import pytest
from contextlib import contextmanager

from esgpull.facet import Facet


@pytest.fixture
def facet():
    return Facet("facet", default="*")


def test_reset_default(facet):
    assert facet.isdefault()
    facet._set("value")
    assert not facet.isdefault()
    facet.reset()
    assert facet.isdefault()


def test_repr(facet):
    assert str(facet) == "facet={'*'}"


def test_fmt_name(facet):
    assert facet.fmt_name == "facet"
    facet.appended = True
    assert facet.fmt_name == "+facet"


def test_dump_default(facet):
    assert facet.dump() == {"facet": "*"}


@contextmanager
def nothing():
    yield


@pytest.mark.parametrize(
    "values,expected,error",
    [
        pytest.param("value", {"value"}, nothing(), id="str"),
        pytest.param(["a", "a"], {"a"}, nothing(), id="list with duplicate"),
        pytest.param(("a", "b"), {"a", "b"}, nothing(), id="tuple"),
        pytest.param({"a", "b"}, {"a", "b"}, nothing(), id="set"),
        pytest.param(0, {}, pytest.raises(ValueError), id="raises"),
    ],
)
def test_value_set_cast(values, expected, error, request):
    facet = request.getfixturevalue("facet")
    with error:
        facet._set(values)
        assert facet.values == expected


def test_iadd(facet):
    assert len(facet.values) == 1
    facet += "one"
    assert len(facet.values) == 1
    facet += "two"
    assert len(facet.values) == 2
    facet += "one"
    assert len(facet.values) == 2
    facet._set("one")
    assert len(facet.values) == 1
