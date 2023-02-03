import pytest

from esgpull.graph import Graph
from esgpull.models import Query


@pytest.fixture
def base():
    base = Query()
    base.selection = dict(
        project="CMIP5",
        ensemble="r1i1p1",
        realm="atmos",
    )
    base.compute_sha()
    return base


@pytest.fixture
def a(base):
    a = Query(require=base.sha)
    a.selection = dict(
        experiment=["historical", "rcp26"],
        time_frequency="mon",
        variable="tasmin",
    )
    a.compute_sha()
    return a


@pytest.fixture
def b(base):
    b = Query(require=base.sha)
    b.selection = dict(
        experiment="rcp85",
        time_frequency="day",
        variable=["tas", "ua"],
    )
    b.compute_sha()
    return b


@pytest.fixture
def c(base):
    c = Query(require=base.sha)
    c.selection = dict(
        time_frequency=["day", "mon", "fx"],
        variable="tasmax",
    )
    c.compute_sha()
    return c


@pytest.fixture
def graph(base, a, b, c):
    graph = Graph(db=None)
    graph.add(base, a, b, c)
    return graph


def test_add_all(base, a, b, c):
    graph = Graph(db=None)
    graph.add(base, a, b, c)
    assert len(graph.queries) == 4
    root_queries = graph.get_children(None)
    base_children = graph.get_children(base.sha)
    assert len(graph.queries) == 4
    assert len(root_queries) == 1
    assert len(base_children) == 3


def test_add_1_by_1(base, a, b, c):
    graph = Graph(db=None)
    graph.add(base)
    graph.add(a)
    graph.add(b)
    graph.add(c)
    assert len(graph.queries) == 4
    root_queries = graph.get_children(None)
    base_children = graph.get_children(base.sha)
    assert len(graph.queries) == 4
    assert len(root_queries) == 1
    assert len(base_children) == 3


def test_add_1_by_1_reverse(base, a, b, c):
    graph = Graph(db=None)
    graph.add(c)
    graph.add(b)
    graph.add(a)
    graph.add(base)
    assert len(graph.queries) == 4
    root_queries = graph.get_children(None)
    base_children = graph.get_children(base.sha)
    assert len(graph.queries) == 4
    assert len(root_queries) == 1
    assert len(base_children) == 3


def test_asdict(graph, base, a, b, c):
    assert graph.asdict() == {
        base.sha: dict(
            selection=dict(
                project="CMIP5",
                ensemble="r1i1p1",
                realm="atmos",
            )
        ),
        a.sha: dict(
            require=base.sha,
            selection=dict(
                experiment=["historical", "rcp26"],
                time_frequency="mon",
                variable="tasmin",
            ),
        ),
        b.sha: dict(
            require=base.sha,
            selection=dict(
                experiment="rcp85",
                time_frequency="day",
                variable=["tas", "ua"],
            ),
        ),
        c.sha: dict(
            require=base.sha,
            selection=dict(
                time_frequency=sorted(["day", "mon", "fx"]),
                variable="tasmax",
            ),
        ),
    }


def test_dump(graph, base, a, b, c):
    assert graph.dump() == [
        dict(
            selection=dict(
                project="CMIP5",
                ensemble="r1i1p1",
                realm="atmos",
            )
        ),
        dict(
            require=base.sha,
            selection=dict(
                experiment=["historical", "rcp26"],
                time_frequency="mon",
                variable="tasmin",
            ),
        ),
        dict(
            require=base.sha,
            selection=dict(
                experiment="rcp85",
                time_frequency="day",
                variable=["tas", "ua"],
            ),
        ),
        dict(
            require=base.sha,
            selection=dict(
                time_frequency=sorted(["day", "mon", "fx"]),
                variable="tasmax",
            ),
        ),
    ]


def test_expand(graph, base, a, b, c):
    assert graph.expand(base.sha).asdict() == base.asdict()
    assert graph.expand(a.sha).asdict() == dict(
        selection=dict(
            project="CMIP5",
            ensemble="r1i1p1",
            realm="atmos",
            experiment=["historical", "rcp26"],
            time_frequency="mon",
            variable="tasmin",
        )
    )
    assert graph.expand(b.sha).asdict() == dict(
        selection=dict(
            project="CMIP5",
            ensemble="r1i1p1",
            realm="atmos",
            experiment="rcp85",
            time_frequency="day",
            variable=["tas", "ua"],
        )
    )
    assert graph.expand(c.sha).asdict() == dict(
        selection=dict(
            project="CMIP5",
            ensemble="r1i1p1",
            realm="atmos",
            time_frequency=sorted(["day", "mon", "fx"]),
            variable="tasmax",
        )
    )
