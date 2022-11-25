from __future__ import annotations

from attrs import Factory, define
from cattrs import Converter

from esgpull.query.options import Option, Options
from esgpull.query.select import Select


@define
class Query:
    name: str
    require: str | None = None
    select: Select = Factory(Select)
    options: Options = Factory(Options)
    transient: bool = False  # if True, do no install from this one


@define(init=False)
class Selection:
    queries: dict[str, Query]

    def __init__(self, *queries: Query) -> None:
        self.queries = {}
        for query in queries:
            self.queries[query.name] = query

    def __getitem__(self, key: str) -> Query:
        return self.queries[key]

    def dump(self) -> list[dict[str, str]]:
        return []


_converter = Converter()
_converter.register_structure_hook(Select, lambda v, _: Select.from_dict(**v))
_converter.register_structure_hook(Option, lambda v, _: Option.parse(v))


if __name__ == "__main__":
    import rich
    import yaml
    from exceptiongroup import print_exception

    content = """
    - name: CMIP5_hist_rcp85_atmos
    select: # alternative names : main/query/facets
        project: CMIP5
        experiment:
        - historical
        - rcp85
        ensemble: r1i1p1
        realm: atmos
    options:
        distrib: true
        retracted: false
    - name: day_tas
    require: CMIP5_hist_rcp85_atmos # alternative names: use/import/from/load
    select:
        time_frequency: day
        variable: tas
    - name: mon_tasmin
    require: CMIP5_hist_rcp85_atmos
    select:
        time_frequency: mon
        variable: tasmin
    """

    try:
        data: list[dict] = yaml.safe_load(content)
        query_list: list[Query] = _converter.structure(data, list[Query])
        selection = Selection(*query_list)
        rich.print(selection)
    except Exception as e:
        print_exception(e)
