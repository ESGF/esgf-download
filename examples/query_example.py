import rich
import sqlalchemy as sa

# import yaml
# from rich.panel import Panel
# from rich.syntax import Syntax
from sqlalchemy.orm import Session

from esgpull.graph import Graph
from esgpull.models import Query

main = Query(
    tags="main",
    selection=dict(
        project="CMIP5",
        # experiment="historical",
        ensemble="r1i1p1",
        realm="atmos",
    ),
    options=dict(
        distrib=False,
        latest=True,
    ),
    transient=True,
)

day = Query(
    tags="day",
    require="main",
    selection=dict(time_frequency="day"),
    transient=True,
)

tasmax_monfx = Query(
    # tags="tasmax_monfx",
    tags=["tasmax", "monfx"],
    require="main",
    selection={"time_frequency": ["mon", "fx"], "variable": "tasmax"},
)

tasmax_day = Query(tags="tasmax", require="day")
tasmax_day.selection.experiment = "historical"  # type: ignore [attr-defined]
tasmax_day.selection.variable = "tasmax"  # type: ignore [attr-defined]

rcp26 = Query(tags="rcp26", require="main")
rcp26.selection.experiment = "rcp26"  # type: ignore [attr-defined]
rcp26.selection.time_frequency = "mon"  # type: ignore [attr-defined]
rcp26.selection.variable = "tasmin"  # type: ignore [attr-defined]


rcp85 = Query(require="day")
rcp85.selection.experiment = "rcp85"  # type: ignore [attr-defined]
rcp85.selection.variable = ["tas", "ua"]  # type: ignore [attr-defined]

other = Query(tags="other", selection=dict(project="CMIP6"))

queries = [main, day, rcp26, tasmax_day, tasmax_monfx, rcp85, other]

# main.compute_sha()
# day.compute_sha()
# rcp26.compute_sha()
# rcp85.compute_sha()
# rich.print(main)
# rich.print(rcp26)
# rich.print(rcp85)
# rich.print(main << rcp26)
# rich.print(main << day << rcp85)

# main.compute_sha()
# rcp26.compute_sha()
# rcp85.compute_sha()
# rich.print(f"my query looks like {main}")
# rich.print(Panel.fit(main))
# rich.print(rcp26)
# rich.print(rcp85)

url = "sqlite:////home/srodriguez/ipsl/esg-pull/examples/graph.db"
engine = sa.create_engine(url)
session = Session(engine)
eqs = " " + "=" * 10 + " "

graph = Graph(session)
rich.print(graph)

# replaced = graph.add(
#     *queries,
#     force=True,
# )

# new_queries = graph.save_new_records()
# for name in new_queries:
#     print(eqs + "replacing" + eqs)
#     rich.print(replaced[name])
#     rich.print(graph[name])
#     print()

# # TODO: fix require using sha not tags[0]
# print(eqs + "YAML" + eqs)
# rich.print(Syntax(yaml.dump(graph.dump()), "yaml", theme="ansi_dark"))
# print()

# # TODO: fix sha not computed
# print(eqs + "FULL GRAPH" + eqs)
# rich.print(graph)
# print()

# # TODO: fix KeyError 'day'
# print(eqs + "SUBGRAPH FROM day" + eqs)
# rich.print(graph.get_tree_from("day"))
# print()

# name = rcp85.name
# print(eqs + f"graph[{name}]" + eqs)
# rich.print(graph[name])
# print()

# print(eqs + f"graph.expand({name})" + eqs)
# rich.print(graph.expand(name))
# print()
