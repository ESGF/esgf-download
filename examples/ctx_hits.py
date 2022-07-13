from esgpull import Context

c = Context(distrib=False, latest=True)
f = c.facets

f.project = "CMIP5"
f.experiment = "historical"
f.ensemble = "r1i1p1"
f.realm = "atmos"
f.time_frequency = "day"

with f:
    # Add to already set experiment with `+=`
    f.experiment += "rcp26"
    f.time_frequency = "mon"
    f.variable = "tasmin"

with f:
    f.experiment = "rcp85"
    f.variable = ["tas", "ua"]

with f:
    f.time_frequency += ["mon", "fx"]
    f.variable = "tasmax"

f.reduce()
print(f)
print(c.hits)
print(len(c.search()))


f._setdefault(full=True)

f.project = "CMIP6"
f.experiment_id = "historical"
f.variant_label = "r1i1p1f1"
# f.realm = "atmos"
f.frequency = "day"

with f:
    # Add to already set experiment with `+=`
    # f.experiment_id += "rcp26"
    f.experiment_id += "ssp245"
    f.frequency = "mon"
    f.variable_id = "tasmin"

with f:
    # f.experiment_id = "rcp85"
    f.experiment_id = "ssp126"
    f.variable_id = ["tas", "ua"]

with f:
    f.frequency += ["mon", "fx"]
    f.variable_id = "tasmax"


f.reduce()
print(f)
print(c.hits)
print(len(c.search()))
