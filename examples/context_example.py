from esgpull.context import Context
from esgpull.query import Query

c = Context(distrib=False, latest=True)
q = Query()

q.project = "CMIP5"
q.experiment = "historical"
q.ensemble = "r1i1p1"
q.realm = "atmos"
q.time_frequency = "day"

qq = q.add()
# Add to already set experiment with `+`
qq.experiment + "rcp26"
qq.time_frequency = "mon"
qq.variable = "tasmin"

qq = q.add()
qq.experiment = "rcp85"
qq.variable = ["tas", "ua"]

qq = q.add()
qq.time_frequency + ["mon", "fx"]
qq.variable = "tasmax"

c.query = q

print(q.dump())
print(c.hits)
print(len(c.search()))

######################

c = Context(distrib=False, latest=True)
q = Query()

q.project = "CMIP6"
q.experiment_id = "historical"
q.variant_label = "r1i1p1f1"
# q.realm = "atmos"
q.frequency = "day"

qq = q.add()
# Add to already set experiment with `+`
# q.experiment_id + "rcp26"
qq.experiment_id + "ssp245"
qq.frequency = "mon"
qq.variable_id = "tasmin"

qq = q.add()
# q.experiment_id = "rcp85"
qq.experiment_id = "ssp126"
qq.variable_id = ["tas", "ua"]

qq = q.add()
qq.frequency + ["mon", "fx"]
qq.variable_id = "tasmax"

c.query = q

print(q.dump())
print(c.hits)
print(len(c.search()))
