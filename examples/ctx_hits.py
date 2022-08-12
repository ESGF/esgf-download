from esgpull.context import Context

c = Context(distrib=False, latest=True)
q = c.query

q.project = "CMIP5"
q.experiment = "historical"
q.ensemble = "r1i1p1"
q.realm = "atmos"
q.time_frequency = "day"

qq = q.add()
# Add to already set experiment with `+=`
qq.experiment += "rcp26"
qq.time_frequency = "mon"
qq.variable = "tasmin"

qq = q.add()
qq.experiment = "rcp85"
qq.variable = ["tas", "ua"]

qq = q.add()
qq.time_frequency += ["mon", "fx"]
qq.variable = "tasmax"

# q.reduce()
print(q)
print(c.hits)
print(len(c.search()))

######################

c = Context(distrib=False, latest=True)
q = c.query

q.project = "CMIP6"
q.experiment_id = "historical"
q.variant_label = "r1i1p1f1"
# q.realm = "atmos"
q.frequency = "day"

qq = q.add()
# Add to already set experiment with `+=`
# q.experiment_id += "rcp26"
q.experiment_id += "ssp245"
q.frequency = "mon"
q.variable_id = "tasmin"

qq = q.add()
# q.experiment_id = "rcp85"
q.experiment_id = "ssp126"
q.variable_id = ["tas", "ua"]

qq = q.add()
q.frequency += ["mon", "fx"]
q.variable_id = "tasmax"

# q.reduce()
print(q)
print(c.hits)
print(len(c.search()))
