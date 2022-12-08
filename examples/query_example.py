import rich
import sqlalchemy as sa
import yaml
from rich.panel import Panel
from rich.syntax import Syntax
from sqlalchemy.orm import Session

from esgpull.graph import Graph
from esgpull.models import Query

main = Query(
    tags="main",
    select=dict(
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
    select=dict(time_frequency="day"),
    transient=True,
)

rcp26 = Query(tags="rcp26", require="main")
rcp26.select.experiment = "rcp26"
rcp26.select.time_frequency = "mon"
rcp26.select.variable = "tasmin"

tasmax_day = Query(tags="tasmax", require="day")
tasmax_day.select.experiment = "historical"
tasmax_day.select.variable = "tasmax"

tasmax_monfx = Query(
    tags="tasmax_monfx",
    # tags=["tasmax", "monfx"],
    require="main",
    select={"time_frequency": ["mon", "fx"], "variable": "tasmax"},
)

rcp85 = Query(require="day")
rcp85.select.experiment = "rcp85"
rcp85.select.variable = ["tas", "ua"]

other = Query(tags="other", select=dict(project="CMIP6"))

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

# # fmt: off
# multitree_graph = """
#               ┌────┬───────────┐                   ┌────┐              ┌─────┬────────┐
#               │main│ transient │                   │sven│              │other│        │
#               ├────┘           │                   └────┤              ├─────┘        │
#               │distrib=false   │                        │              │project: CMIP6│
#               │latest=true     │                        │              └──────────────┘
#               ├──              │                        │
#               │ensemble: r1i1p1│                        │
#               │project: CMIP5  │                        │
#               │realm: atmos    │                        │
#               ├────────────────┘                        │
#               │                                         │
#         ┌─────┴──────────────┬─────────────────┐ ┌──────┴───────────┐
#         │                    │                 │ │                  │
#         ▼                    ▼                 ▼ ▼                  │
#     ┌───┬───────────────┐ ┌────────────┬───┐  ┌─────┬─────────────┐ │
#     │day│     transient │ │tasmax_monfx│   │  │rcp26│             │ │
#     ├───┘               │ ├────────────┘   │  ├─────┘             │ │
#     │time_frequency: day│ │time_frequency: │  │experiment: rcp26  │ │
#     ├───────────────────┘ │- mon           │  │time_frequency: mon│ │
#     │                     │- fx            │  │variable: tasmin   │ │
#     ▼                     │variable: tasmax│  └───────────────────┘ │
# ┌──────┬───────────────┐  └────────────────┘                        │
# │tasmax│               │                                            │
# ├──────┘               │                                            ▼
# │experiment: historical│                                         ┌─────┬─────────────┐
# │variable: tasmax      │                                         │rcp85│             │
# └──────────────────────┘                                         ├─────┘             │
#                                                                  │experiment: rcp85  │
#                                                                  │variable: [tas, ua]│
#                                                                  │- tas              │
#                                                                  │- ua               │
#                                                                  └───────────────────┘
# """
# # fmt: on

# # --> no multitree, instead add tags with tag+query_tag table
