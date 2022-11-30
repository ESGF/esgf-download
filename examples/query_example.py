import rich
import yaml
from rich.panel import Panel
from rich.syntax import Syntax

from esgpull.query import Query
from esgpull.query.query import Selection

main = Query(
    "main",
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
    "day",
    require="main",
    select=dict(time_frequency="day"),
    transient=True,
)

rcp26 = Query("rcp26", require="main")
rcp26.select.experiment = "rcp26"
rcp26.select.time_frequency = "mon"
rcp26.select.variable = "tasmin"

tasmax_day = Query("tasmax", require="day")
tasmax_day.select.experiment = "historical"
tasmax_day.select.variable = "tasmax"

tasmax_monfx = Query(
    "tasmax_monfx",
    require="main",
    select={"time_frequency": ["mon", "fx"], "variable": "tasmax"},
)

rcp85 = Query("rcp85", require="day")
rcp85.select.experiment = "rcp85"
rcp85.select.variable = "tas", "ua"

other = Query("other", select=dict(project="CMIP6"))

# rich.print(f"my query looks like {main}")
# rich.print(Panel.fit(main))
# rich.print(rcp26)
# rich.print(rcp85)

selection = Selection(
    main,
    day,
    rcp26,
    tasmax_day,
    tasmax_monfx,
    rcp85,
    other,
)
eqs = " " + "=" * 10 + " "
print(eqs + "YAML" + eqs)
rich.print(Syntax(yaml.dump(selection.dump()), "yaml", theme="ansi_dark"))
print()
print(eqs + "FULL GRAPH" + eqs)
rich.print(selection)
print()
print(eqs + "SUBGRAPH FROM day" + eqs)
rich.print(selection.get_tree_from("day"))

# rich.print(main)
# rich.print(rcp26)
# rich.print(rcp85)
# rich.print(main << rcp26)
# rich.print(main << day << rcp85)

# fmt: off
multitree_graph = """
              ┌────┬───────────┐                   ┌────┐              ┌─────┬────────┐
              │main│ transient │                   │sven│              │other│        │
              ├────┘           │                   └────┤              ├─────┘        │
              │distrib=false   │                        │              │project: CMIP6│
              │latest=true     │                        │              └──────────────┘
              ├──              │                        │
              │ensemble: r1i1p1│                        │
              │project: CMIP5  │                        │
              │realm: atmos    │                        │
              ├────────────────┘                        │
              │                                         │
        ┌─────┴──────────────┬─────────────────┐ ┌──────┴───────────┐
        │                    │                 │ │                  │
        ▼                    ▼                 ▼ ▼                  │
    ┌───┬───────────────┐ ┌────────────┬───┐  ┌─────┬─────────────┐ │
    │day│     transient │ │tasmax_monfx│   │  │rcp26│             │ │
    ├───┘               │ ├────────────┘   │  ├─────┘             │ │
    │time_frequency: day│ │time_frequency: │  │experiment: rcp26  │ │
    ├───────────────────┘ │- mon           │  │time_frequency: mon│ │
    │                     │- fx            │  │variable: tasmin   │ │
    ▼                     │variable: tasmax│  └───────────────────┘ │
┌──────┬───────────────┐  └────────────────┘                        │
│tasmax│               │                                            │
├──────┘               │                                            ▼
│experiment: historical│                                         ┌─────┬─────────────┐
│variable: tasmax      │                                         │rcp85│             │
└──────────────────────┘                                         ├─────┘             │
                                                                 │experiment: rcp85  │
                                                                 │variable: [tas, ua]│
                                                                 │- tas              │
                                                                 │- ua               │
                                                                 └───────────────────┘
"""
# fmt: on

# --> no multitree, instead add tags with tag+query_tag table
