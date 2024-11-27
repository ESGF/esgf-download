import pytest

from esgpull.exceptions import AlreadySetFacet, DuplicateFacet
from esgpull.models import Selection


@pytest.fixture
def selection():
    Selection.configure("a", "b", "c", "d", replace=True)
    yield Selection()
    Selection.reset()


def test_configure():
    Selection.configure("some", "thing")
    new_names = {"some", "thing", "!some", "!thing"}
    assert new_names <= Selection._facet_names
    Selection.configure("some", "thing", replace=True)
    assert new_names == Selection._facet_names
    sel = Selection()
    with pytest.raises(KeyError):
        assert sel["a"] == []
    Selection.configure("a")  # add 'a' to facets
    assert sel["a"] == []  # no more raise
    Selection.reset()


def test_basic(selection):
    selection["a"] = "value"
    selection["b"] = ["1", "2", "3"]
    selection["c"] = "val_b", "val_a"
    assert selection["a"] == ["value"]  # single value is cast to list
    assert selection["b"] == ["1", "2", "3"]  # unchanged
    assert selection["c"] == ["val_a", "val_b"]  # sorted values
    assert selection["d"] == []  # unset is empty values


def test_wrong_facet_name(selection):
    with pytest.raises(KeyError):
        selection["e"] = "value"
    with pytest.raises(KeyError):
        selection["f"] == []


def test_attr(selection):
    selection.a = "value"
    assert selection.a == ["value"]


def test_already_set_facet(selection):
    selection.a = "value"
    with pytest.raises(AlreadySetFacet):
        selection.a = "other_value"


def test_duplicate_facet(selection):
    with pytest.raises(DuplicateFacet):
        selection.b = "value", "value"


def test_dump(selection):
    selection.a = "value"
    selection["b"] = ["1", "2", "3"]
    selection.c = "val_b", "val_a"
    assert selection.asdict() == {
        "a": "value",
        "b": ["1", "2", "3"],
        "c": ["val_a", "val_b"],
    }
    selection.name = "other_value"
    assert "name" not in selection.asdict()


def test_ignore_facet(selection):
    selection["!a"] = "value"
    assert selection.asdict() == {"!a": "value"}


def test_ignore_facet_already_set(selection):
    selection.a = "value"
    with pytest.raises(AlreadySetFacet):
        selection["!a"] = "other_value"
