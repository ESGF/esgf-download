from pprint import pprint
from esgpull.context.facets import Facets

from query import Query

q = Query()

# Set facets using `=`
q.project = "CMIP5"
q.experiment = "historical"
q.ensemble = "r1i1p1"
q.realm = "atmos"
q.time_frequency = "day"

qq = q.add()
# Appending to previously set facet is done with `+=`
qq.experiment += "rcp26"
# Setting the facet with `=` replaces the previous value
qq.time_frequency = "mon"
qq.variable = "tasmin"

qq = q.add()
qq.experiment = "rcp85"
qq.variable = ["tas", "ua"]

qq = q.add()
qq.time_frequency += ["mon", "fx"]
qq.variable = "tasmax"

# fmt:off

pprint(q.dump())

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

pprint([flat.dump() for flat in q.flatten()])

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

# q.reduce()
# pprint(q.dump())

# {'ensemble': 'r1i1p1',
#  'experiment': 'historical',
#  'project': 'CMIP5',
#  'realm': 'atmos',
#  'requests': [{'+experiment': 'rcp26',
#                'time_frequency': 'mon',
#                'variable': 'tasmin'},
#               {'experiment': 'rcp85',
#                'time_frequency': 'day',
#                'variable': 'tas,ua'},
#               {'+time_frequency': 'fx', 'variable': 'tasmax'}],
#  'time_frequency': 'day,mon'}
# # fmt: on
