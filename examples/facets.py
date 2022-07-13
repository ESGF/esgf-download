from pprint import pprint
from esgpull.context.facets import Facets

f = Facets()

# Set facets using `=`
f.project = "CMIP5"
f.experiment = "historical"
f.ensemble = "r1i1p1"
f.realm = "atmos"

# Alternatively, set facets using `<`
f.time_frequency < "day"

with f:
    # Appending to previously set facet is done with `+=`
    f.experiment += "rcp26"
    # Setting the facet with `=` replaces the previous value
    f.time_frequency = "mon"
    f.variable = "tasmin"

with f:
    f.experiment = "rcp85"
    f.variable = ["tas", "ua"]

with f:
    # Appending also works with `<<` (unix syntax)
    f.time_frequency << ["mon", "fx"]
    # Setting also work with `<` (unix syntax)
    f.variable < "tasmax"

# fmt:off

pprint(f.dump())

{'ensemble': 'r1i1p1',
 'experiment': 'historical',
 'project': 'CMIP5',
 'realm': 'atmos',
 'requests': [{'+experiment': 'rcp26',
               'time_frequency': 'mon',
               'variable': 'tasmin'},
              {'experiment': 'rcp85', 'variable': 'tas,ua'},
              {'+time_frequency': 'fx,mon', 'variable': 'tasmax'}],
 'time_frequency': 'day'}

pprint(f.dump_flat())

[{'ensemble': 'r1i1p1',
  'experiment': 'rcp26,historical',
  'project': 'CMIP5',
  'realm': 'atmos',
  'time_frequency': 'mon',
  'variable': 'tasmin'},
 {'ensemble': 'r1i1p1',
  'experiment': 'rcp85',
  'project': 'CMIP5',
  'realm': 'atmos',
  'time_frequency': 'day',
  'variable': 'tas,ua'},
 {'ensemble': 'r1i1p1',
  'experiment': 'historical',
  'project': 'CMIP5',
  'realm': 'atmos',
  'time_frequency': 'day,fx,mon',
  'variable': 'tasmax'}]

f.reduce()
pprint(f.dump())

{'ensemble': 'r1i1p1',
 'experiment': 'historical',
 'project': 'CMIP5',
 'realm': 'atmos',
 'requests': [{'+experiment': 'rcp26',
               'time_frequency': 'mon',
               'variable': 'tasmin'},
              {'experiment': 'rcp85',
               'time_frequency': 'day',
               'variable': 'tas,ua'},
              {'+time_frequency': 'fx', 'variable': 'tasmax'}],
 'time_frequency': 'day,mon'}
# fmt: on
