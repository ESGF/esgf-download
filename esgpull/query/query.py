from __future__ import annotations

from collections import Counter
from typing import Any, Iterator

from attrs import define, field
from cattrs import Converter, gen
from rich.console import Console, ConsoleOptions
from rich.measure import Measurement, measure_renderables
from rich.padding import Padding
from rich.pretty import pretty_repr
from rich.text import Text
from rich.tree import Tree

from esgpull.exceptions import QueryNameCollision
from esgpull.query.facets import Select
from esgpull.query.options import Option, Options


@define(slots=False)
class Query:
    name: str
    transient: bool = field(default=False)  # when True, query not installed
    require: str | None = field(default=None)
    select: Select = field(
        factory=Select,
        converter=lambda s: Select.new(s),
    )
    options: Options = field(
        factory=Options,
        converter=lambda o: Options.new(o),
    )
    _rich_require: bool = field(init=False, default=True)

    def asdict(self) -> dict[str, Any]:
        return _converter.unstructure(self)

    def clone(self) -> Query:
        return _converter.structure(self.asdict(), Query)

    def no_require(self) -> Query:
        cl = self.clone()
        cl._rich_require = False
        return cl

    def __lshift__(self, other: Query) -> Query:
        names = self.name.split("+")
        curname = names[-1]
        if curname != other.require:
            raise ValueError(f"{curname} is not required by {other.name}")
        result = self.clone()
        result.name += f"+{other.name}"
        for name, facet in other.select.items():
            result.select[name] = facet
        result.transient = other.transient
        return result

    def __rich_repr__(self) -> Iterator:
        yield "name", self.name
        if self.require:
            yield "require", self.require
        if self.transient:
            yield "transient", self.transient
        if self.options:
            yield "options", self.options
        if self.select:
            yield "select", self.select

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        items = [f"{k}={v}" for k, v in self.__rich_repr__()]
        return f"{cls_name}(" + ", ".join(items) + ")"

    def __rich_console__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> Iterator[Text | Padding]:
        def guide(t: Text, size: int = 2) -> Text:
            return t.with_indent_guides(size, style="dim default")

        text = Text()
        text.append(self.name, style="b green")
        if self.transient:
            text.append(" <transient>", style="i red")
        if self._rich_require and self.require is not None:
            text.append(" [require: ")
            text.append(self.require, style="green")
            text.append("]")
        yield text
        for name, option in self.options.items():
            text = Text("  ")
            text.append(name, style="yellow")
            text.append(f": {option.value}")
            yield guide(text)
        for name, facet in self.select.items():
            item = guide(Text(f"  {name}", style="blue"))
            if len(facet) == 1:
                item.append(f": {facet[0]}", style="default")
            else:
                item.append(":")
                for value in facet:
                    item.append(f"\n    - {value}", style="default")
                    item = guide(item, 4)
            yield item

    def __rich_measure__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> Measurement:
        renderables = list(self.__rich_console__(console, options))
        return measure_renderables(console, options, renderables)


@define(init=False, slots=False)
class Selection:
    queries: tuple[Query, ...]
    query_map: dict[str, int] = field(repr=False)
    _rendered: set[int] = field(repr=False)

    def __init__(self, *queries: Query) -> None:
        self.validate(*queries)
        self.queries = queries
        self.query_map = {}
        for i, query in enumerate(queries):
            self.query_map[query.name] = i

    def validate(self, *queries: Query) -> None:
        names = Counter(q.name for q in queries)
        nb_collision = sum(c > 1 for c in names.values())
        if nb_collision == 0:
            return
        collisions = dict(names.most_common(nb_collision))
        collision_dict: dict[str, list[Query]] = {
            name: [] for name in collisions
        }
        for query in queries:
            if sum(collisions.values()) == 0:
                break
            if query.name in collisions:
                collision_dict[query.name].append(query)
                collisions[query.name] -= 1
        raise QueryNameCollision(pretty_repr(collision_dict))

    def add(self, query: Query) -> None:
        self.validate(*self.queries, query)
        self.queries += (query,)
        self.query_map[query.name] = len(self.queries) - 1

    def __getitem__(self, key: str) -> Query:
        return self.queries[self.query_map[key]]

    def expand(self, name: str) -> Query:
        query = self[name]
        while query.require is not None:
            query = self[query.require] << query
        return query

    def dump(self) -> list[dict[str, Any]]:
        return [q.asdict() for q in self.queries]

    def fill_tree_from(self, root: str | None, tree: Tree) -> None:
        for i, query in enumerate(self.queries):
            if i in self._rendered:
                continue
            if root == query.require:
                self._rendered.add(i)
                self.fill_tree_from(query.name, tree.add(query.no_require()))

    def get_tree_from(self, root: str | None) -> Tree:
        tree = Tree("", hide_root=True, guide_style="dim")
        self._rendered = set()
        if root is not None:
            root_idx = self.query_map[root]
            self._rendered.add(root_idx)
            tree = tree.add(self.queries[root_idx])
        self.fill_tree_from(root, tree)
        del self._rendered
        return tree

    def __rich_console__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> Iterator[Tree]:
        yield self.get_tree_from(None)


_converter = Converter(omit_if_default=True, prefer_attrib_converters=True)
_converter.register_unstructure_hook(Select, lambda v: v.asdict())
_converter.register_unstructure_hook(Option, lambda v: Option(v).value)
_converter.register_unstructure_hook(
    Query,
    gen.make_dict_unstructure_fn(
        Query, _converter, _rich_require=gen.override(omit=True)
    ),
)


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

    c = Console()
    cmip5 = Query("cmip5", require="toto")
    cmip5.select.project = "CMIP5"
    cmip5.options.distrib = True
    cmip5.options.latest = True
    json = _converter.unstructure(cmip5)
    fromjson = _converter.structure(json, Query)
    c.print("json", json)
    c.print("fromjson", fromjson)
    c.print("original", cmip5)
    c.print("cmip5 == fromjson:", cmip5 == fromjson)
