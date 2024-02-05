import pytest

from esgpull.database import Database
from esgpull.graph import Graph
from esgpull.models import Query


@pytest.fixture
def base():
    base = Query(
        selection=dict(
            project="CMIP5",
            ensemble="r1i1p1",
            realm="atmos",
        )
    )
    base.compute_sha()
    return base


@pytest.fixture
def a(base):
    a = Query(
        require=base.sha,
        selection=dict(
            experiment=["historical", "rcp26"],
            time_frequency="mon",
            variable="tasmin",
        ),
    )
    a.compute_sha()
    return a


@pytest.fixture
def b(base):
    b = Query(
        require=base.sha,
        selection=dict(
            experiment="rcp85",
            time_frequency="day",
            variable=["tas", "ua"],
        ),
    )
    b.compute_sha()
    return b


@pytest.fixture
def c(base):
    c = Query(
        require=base.sha,
        selection=dict(
            time_frequency=["day", "mon", "fx"],
            variable="tasmax",
        ),
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


@pytest.fixture
def graph2():
    graph = Graph(db=None)
    cordex = Query(selection=dict(project="CORDEX"))
    cordex.compute_sha()
    cmip5 = Query(selection=dict(project="CMIP5"))
    cmip5.compute_sha()
    cmip5_tas = Query(require=cmip5.sha, selection=dict(variable_id="tas"))
    cmip5_tas.compute_sha()
    cmip5_pr = Query(require=cmip5.sha, selection=dict(variable_id="pr"))
    cmip5_pr.compute_sha()
    cmip6 = Query(selection=dict(project="CMIP6"), tags=["cmip6"])
    cmip6.compute_sha()
    cmip6.sha = "bad_value"
    cmip6_tas = Query(
        require=cmip6.sha, selection=dict(variable_id="tas"), tags=["children"]
    )
    cmip6_tas.compute_sha()
    cmip6_pr = Query(
        require=cmip6.sha, selection=dict(variable_id="pr"), tags=["children"]
    )
    cmip6_pr.compute_sha()
    cmip6_pr_member = Query(
        require=cmip6_pr.sha, selection=dict(member_id="r1i1p1f1")
    )
    cmip6_pr_member.compute_sha()
    graph.add(
        cordex,
        cmip5,
        cmip5_tas,
        cmip5_pr,
        cmip6,
        cmip6_tas,
        cmip6_pr,
        cmip6_pr_member,
        clone=False,
    )
    return graph


def test_children(graph2):
    assert len(graph2.get_children("bad_value")) == 2
    assert len(graph2.get_all_children("bad_value")) == 3


@pytest.fixture
def db(config):
    return Database.from_config(config)


def test_children_db(graph2, db):
    graph = Graph(db)
    graph.add(*graph2.queries.values(), clone=False)
    graph.merge()
    graph = Graph(db)  # reset graph
    assert len(graph.queries) == 0
    assert len(graph.get_children("bad_value")) == 2
    assert len(graph.get_all_children("bad_value")) == 3


def test_delete_add_merge(base, db):
    graph1 = Graph(db)
    graph1.add(base)
    graph1.merge()
    graph2 = Graph(db)
    assert base in graph2
    graph2.delete(base)
    graph2.add(base)
    graph2.merge()
    graph3 = Graph(db)
    assert base in graph3
