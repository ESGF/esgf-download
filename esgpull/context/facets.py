from __future__ import annotations
from typing import Iterator, Optional, TypeAlias, Any, cast

import yaml
from pathlib import Path

# from pyesgf.search import SearchContext

from esgpull.context.constants import DEFAULT_FACETS, EXTRA_FACETS
from esgpull.utils import errors

FacetDefault: str = "*"
FacetValues: TypeAlias = str | set[str] | list[str]
Use: TypeAlias = dict[str, str]
Dump: TypeAlias = dict[str, str | Use | list[Use]]


class Facet:
    def __init__(self, name: str, default: FacetValues) -> None:
        self.name = name
        self.values = self._cast(default)
        self.default = frozenset(self.values)
        self.appended = False

    def setdefault(self) -> None:
        self.values = set(self.default)

    def isdefault(self) -> bool:
        return self.values == self.default

    @property
    def fmt_name(self) -> str:
        name = self.name
        if self.appended:
            name = "+" + name
        return name

    def dump(self) -> Use:
        return {self.fmt_name: str(self)}

    def __repr__(self) -> str:
        return ",".join(self.values)

    def tostring(self) -> str:
        result = str(self)
        if len(self.values) > 1:
            result = "[" + result + "]"
        return f"{self.fmt_name}:{result}"

    def _cast(self, values: FacetValues) -> set[str]:
        match values:
            case set() | list() | tuple():
                result = set(values)
            case str():
                result = set(map(str.strip, values.split(",")))
            case _:
                raise ValueError(self.name, values)
        return result

    def _set(self, values: FacetValues | Facet) -> None:
        """
        We can only "overload" the `=` operator with a property, so this
        method implements the replacement, and is called from `__set__`.
        """
        if isinstance(values, Facet):
            return  # Required for `+=` / `__iadd__`.
        self.values = self._cast(values)
        self.appended = False

    def _append(self, values: FacetValues) -> None:
        """
        Append values, replace if default.
        """
        values = self._cast(values)
        if self.isdefault():
            self.values = values
        else:
            self.values |= values
        self.appended = True

    # def __lt__(self, values: FacetValues | Facet) -> None:
    #     """
    #     We cannot overload the `=` operator directly, so I chose `<` as the
    #     main assignment operator (though `=` works fine with properties).

    #     Example:
    #         ```python
    #         f = Facet("name", default="*")
    #         print(f.tostring())
    #         # name: *
    #         f < "value"
    #         print(f.tostring())
    #         # name: new_value
    #         ```
    #     """
    #     self._set(values)

    # def __lshift__(self, values: FacetValues) -> Facet:
    #     """
    #     Define `<<` as appending operator.
    #     Can be chained to append multiple values. (keep?)

    #     Example:
    #         ```python
    #         f = Facet("name", default="*")
    #         f << "first" << "second"
    #         print(f.tostring())
    #         # name: [second,first]
    #         ```
    #     """
    #     self._append(values)
    #     return self  # enable chain `<<`, only works for this operator

    def __iadd__(self, values: FacetValues) -> Facet:
        """
        Define `+=` as appending operator.

        Example:
            ```python
            f = Facet("name", default="*")
            f += "first"
            f += "second"
            print(f.tostring())
            # name: [second,first]
            ```
        """
        self._append(values)
        return self


class State:
    def __init__(
        self,
        d: dict[str, Any] = None,
        *,
        use: Use = None,
        requests: list[Use] = None,
    ) -> None:
        self.use: Use
        self.requests: list[Use]

        if d is None and use is not None and requests is not None:
            self.use = use
            self.requests = requests
            return

        d = cast(dict, d)
        match d:
            case {"use": dict(use), "requests": list(requests)}:
                self.use = use
                self.requests = requests
            case {"requests": list(requests), **use}:
                self.use = use
                self.requests = requests
            case dict():
                self.use = d
                self.requests = []
            case _:
                raise ValueError(d, use, requests)


class Facets:
    """
    Interface to get/set facet values.

    Each facet is defined dynamically as a property
    to restrict usage to the controlled vocabulary.

    TODO: recode validations?
    TODO: use `query=<facet>:<pattern>` instead of `<facet>=<value>[,<value>]`
        with pattern atoms: `<value>`, `*`, `OR`, `AND`
    """

    SETUP_DONE = False

    @classmethod
    def define(cls, name: str, default: FacetValues) -> None:
        if cls.SETUP_DONE:
            raise ValueError("Cannot setup twice.")

        def get_facet(self) -> Facet:
            self._facets.setdefault(name, Facet(name, default))
            return self._facets[name]

        def set_facet(self, values: FacetValues) -> None:
            get_facet(self)._set(values)

        setattr(cls, name, property(get_facet, set_facet))

    @classmethod
    def setup(cls, default: list[str]) -> None:
        # default_with_value: dict[str, str] = DEFAULT_CONSTRAINTS_WITH_VALUE,
        for name in default + EXTRA_FACETS:
            cls.define(name, FacetDefault)
        # for name, values in default_with_value.items():
        #     cls.define(name, values)
        cls.SETUP_DONE = True

    @classmethod
    def reset(cls) -> None:
        if not cls.SETUP_DONE:
            raise ValueError("No facets to reset.")

        properties = [
            name
            for name, prop in vars(cls).items()
            if isinstance(prop, property)
        ]
        for name in properties:
            delattr(cls, name)
        cls.SETUP_DONE = False

    def __init__(
        self,
        state: Optional[State | dict] = None,
        /,
        # ctx: SearchContext = None,
        # validations: bool = True,
    ) -> None:
        """
        `validations=True` only works when `ctx` is also provided.
        """
        object.__setattr__(self, "_initialized", False)
        # self._ctx = ctx
        # self._validations = validations
        self._stack: list[Facets] = []
        self.requests: list[Facets] = []
        self._facets: dict[str, Facet] = {}
        for prop in vars(self.__class__).values():
            if isinstance(prop, property):
                prop.__get__(self)  # Fill _facets with Facet instances
        self._initialized = True
        if state is not None:
            self.load(state)

    # def _fetch_options(
    #     self,
    # ) -> Optional[dict[str, set[str]]]:
    #     match self._ctx:
    #         case None:
    #             opts = None
    #         case ctx:
    #             raw_opts = ctx.get_facet_options()
    #             opts = {}
    #             for key, value_dict in raw_opts.items():
    #                 opts[key] = set(value_dict.keys())
    #     return opts

    # def _maybe_validate(
    #     self, name: str, values: set[str]) -> None:
    #     if not self._validations:
    #         return
    #     match self._fetch_options():
    #         case dict(opts):
    #             if name not in opts:
    #                 raise errors.ImpossibleFacet(name, self)
    #             val_opts = {FacetDefault} | opts[name]
    #             if isinstance(values, str):
    #                 values = {values}
    #             for val in values:
    #                 if val not in val_opts:
    #                     raise errors.UnknownFacetValue(val, name)
    #         case None:
    #             ...

    def __iter__(self) -> Iterator[Facet]:
        """
        Iterate over non-default facets, yielding instances of `Facet`.

        Example:
            ```python
            facets = Facets()
            facets.project = "CMIP6"
            for facet in facets:
                print(facet.tostring())
            # project: CMIP6
            ```
        """
        for facet in self._facets.values():
            if not facet.isdefault():
                yield facet

    def __len__(self) -> int:
        return len(list(iter(self)))

    def __getitem__(self, name: str) -> Facet:
        """
        Getter that allows only defined methods or facets, raise otherwise.
        Enables dict-like `[]` behaviour.

        Example:
            ```python
            facets = Facets()
            print(facets["variable_id"])
            # *
            ```

        Example:
            ```python
            facets = Facets()
            print(facets.variable_id)
            # *
            ```
        """
        if name not in self._facets:
            raise errors.UnknownFacetName(name)
        else:
            return object.__getattribute__(self, name)

    def __setitem__(self, name: str, values: FacetValues) -> None:
        """
        Setter that allows only defined facets, raise otherwise.
        Validates `name` and `values` when `validations` is True.
        Enables dict-like `[]=` behaviour.

        Example:
            ```python
            facets = Facets()
            facets["project"] = "CMIP5"
            facets.variable = "ta"
            print(facets)
            # Facets(project: CMIP5, variable: ta)
            ```

        Example:
            ```python
            from esgpull import Context
            ctx = Context()
            ctx.facets.project = "CMIP5"
            ctx.update()

            ctx.facets["mip_era"] = "CMIP6"
            # ImpossibleFacet: mip_era cannot be set with the current facets:
            #     Facets(project: CMIP5)

            ctx.facets.not_facet = "value"
            # UnknownFacetName: 'not_facet' is not a valid facet.
            ```
        """
        if name in self.__dict__:
            object.__setattr__(self, name, values)
        elif name not in self._facets:
            raise errors.UnknownFacetName(name)
        else:
            # self._maybe_validate(name, values)
            object.__setattr__(self, name, values)

    def __getattr__(self, name: str) -> Facet:
        if self._initialized:
            return self[name]
        else:
            return object.__getattribute__(self, name)

    def __setattr__(self, name: str, values: FacetValues) -> None:
        if self._initialized:
            self[name] = values
        else:
            object.__setattr__(self, name, values)

    def _tostring(self, brackets=False) -> str:
        facets = [f.tostring() for f in iter(self)]
        if self.requests:
            requests = [r._tostring(brackets=True) for r in self.requests]
            facets.append(f"requests: {', '.join(requests)}")
        result = ", ".join(facets)
        if brackets:
            result = "{" + result + "}"
        return result

    def __repr__(self) -> str:
        return f"Facets({self._tostring()})"

    def _dump_use(self) -> Use:
        """
        Returns "use" facets as a dict.
        # `State` instance, useful to save state for later `_load`.

        Example:
            ```python
            facets = Facets()
            facets.project = "CMIP6"
            print(facets._dump_use())
            # {"project": "CMIP6"}
            ```
        """
        result: Use = {}
        for facet in iter(self):
            result |= facet.dump()
        return result

    def state(self) -> State:
        """
        Dump `State`.
        """
        use = self._dump_use()
        requests = [r._dump_use() for r in self.requests]
        return State(use=use, requests=requests)

    def dump(self, /, flat_use=True) -> Dump:
        """
        Returns a dict of facets with format:
            ```
            {"use": {<facets>}, "requests": [{<facets>}, ...]}
            ```

        If `flat_use=True` (default=True), the format changes to:
            ```
            {<facets>, "requests": [{<facets>}, ...]}
            ```

        This is used mainly for storing facets as json/yaml.

        Example:
            ```python
            facets = Facets()
            facets.project = "CMIP6"
            facets.variable = "first"
            with facets:
                facets.variable = "second"
            with facets:
                facets.variable << "third"
            print(facets.dump())
            # {'project': 'CMIP6',
            #  'variable': 'first',
            #  'requests': [{'variable': 'second'},
            #               {'+variable': 'third'}]}
            ```
        """
        state = self.state()
        result: Dump = {}
        if flat_use:
            result |= state.use
        else:
            result["use"] = state.use
        if state.requests:
            result["requests"] = state.requests
        return result

    def _load_use(self, use: Use) -> None:
        """
        Load "use" facets.
        """
        for name, values in use.items():
            if name.startswith("+"):
                self[name.removeprefix("+")] += values
            else:
                self[name] = values

    def load(self, state: State | dict | str | Path) -> None:
        """
        Load from either `State`, dict of facets or path.
        """
        if isinstance(state, (str, Path)):
            state = Path(state)
            with state.open() as f:
                state = yaml.safe_load(f)
        if isinstance(state, dict):
            state = State(state)
        state = cast(State, state)  # required by mypy
        # disable validations for speed
        # validations = self._validations
        # self._validations = False
        self._load_use(state.use)
        for request in state.requests:
            with self:
                self._load_use(request)
        # self._validations = validations

    def _setdefault(self, /, full=False) -> None:
        if full:
            self._stack = []
            self.requests = []
        for facet in self:
            facet.setdefault()

    def _clone(self) -> Facets:
        result = Facets()
        for facet in self:
            if facet.appended:
                result[facet.name] += facet.values
            else:
                result[facet.name] = facet.values
        return result

    def _fillwith(self, facets: Facets) -> None:
        for facet in facets:
            if facet.appended:
                self[facet.name] += facet.values
            else:
                self[facet.name] = facet.values

    def __enter__(self) -> Facets:
        self._stack.append(self._clone())
        self._setdefault()
        return self

    def __exit__(self, *exc) -> None:
        if len(self):
            # discard empty request
            self.requests.append(self._clone())
        self._setdefault()
        self._fillwith(self._stack.pop())

    def flatten(self) -> list[Facets]:
        result = []
        for request in self.requests:
            clone = self._clone()
            clone._fillwith(request)
            for facet in clone:
                facet.appended = False
            result.append(clone)
        if not result:
            clone = self._clone()
            for facet in clone:
                facet.appended = False
            result.append(clone)
        return result

    def dump_flat(self) -> list[Use]:
        result = []
        for facets in self.flatten():
            result.append(facets._dump_use())
        return result

    def reduce(self) -> None:
        """
        Factorize (-> `use`) values that are present in more than half of the
        "flat" requests and for which the facet is present in all of them.

        The threshold at half of the requests is a heuristic for a short syntax
        but it could certainly be improved. It still sometimes produces
        non-optimal results dumps (e.g. `time_frequency` in example).
        Use-facets must be set for all requests as it's not possible to remove
        a facet value if it is set in `use`.
        Of course, facets could be reset to the default value, but this kind of
        hack would consequently require additional keywords (`-<facet>`?) for
        the yaml/json syntax and clutter the readability/interpretability.

        TODO: Optimize on reducing number of +facets:
            ```python
            {'requests': [{'time_frequency': 'mon', 'variable': 'tasmin'},
                          {'variable': 'tas,ua'},
                          {'+time_frequency': 'fx,mon', 'variable': 'tasmax'}],
             'time_frequency': 'day'}
            {'requests': [{'time_frequency': 'mon', 'variable': 'tasmin'},
                          {'time_frequency': 'day', 'variable': 'tas,ua'},
                          {'+time_frequency': 'fx', 'variable': 'tasmax'}],
             'time_frequency': 'day,mon'}
            ```

        Example:
            ```python
            facets = Facets()
            with facets:
                facets.institution = "IPSL"
                facets.realm = "atmos"
                facets.experiment = ["rcp26", "historical"]
                facets.time_frequency = "mon"
                facets.variable = "tasmin"
            with facets:
                facets.institution = "IPSL"
                facets.realm = "atmos"
                facets.experiment = ["rcp85", "historical"]
                facets.time_frequency = "day"
                facets.variable = "tas"
            with facets:
                facets.realm = "atmos"
                facets.experiment = "historical"
                facets.time_frequency = ["fx", "day", "mon"]
                facets.variable = "tasmax"
            facets.reduce()
            print(facets.dump())
            # {'experiment': 'historical',
            #  'realm': 'atmos',
            #  'requests': [{'+experiment': 'rcp26',
            #                'institution': 'IPSL',
            #                'time_frequency': 'mon',
            #                'variable': 'tasmin'},
            #               {'+experiment': 'rcp85',
            #                'institution': 'IPSL',
            #                'time_frequency': 'day',
            #                'variable': 'tas'},
            #               {'+time_frequency': 'fx', 'variable': 'tasmax'}],
            #  'time_frequency': 'mon,day'}
            ```
        """
        flat = self.flatten()
        num_requests = len(flat)
        min_for_use = num_requests // 2
        facet_counts: dict[str, int] = {}
        value_counts: dict[str, dict[str, int]] = {}
        common: dict[str, set[str]] = {}
        use: Use = {}
        requests: list[Use] = []

        # First loop: count facets and values
        for facets in flat:
            for facet in facets:
                if facet.isdefault():
                    continue
                values = set(facet.values)
                facet_counts.setdefault(facet.name, 0)
                facet_counts[facet.name] += 1
                for value in values:
                    value_counts.setdefault(facet.name, {})
                    value_counts[facet.name].setdefault(value, 0)
                    value_counts[facet.name][value] += 1

        # Second loop: include facet values meeting the requirements
        for name in facet_counts:
            common.setdefault(name, set())
            if facet_counts[name] < num_requests:
                continue
            for value, count in value_counts[name].items():
                if count > min_for_use:
                    common[name].add(value)

        # Third loop: create `use` dict, nonempty values concatenated to string
        for name, values in common.items():
            if values:
                use[name] = ",".join(values)

        # Fourth loop: create `requests` list, infer values to append
        for facets in flat:
            request: Use = {}
            for facet in facets:
                contains_common = facet.values.issuperset(common[facet.name])
                if facet.name in use and contains_common:
                    # If the set of common values for this facet is a subset
                    # of the full set of values on this request, we can infer
                    # that this facet is appended on this requests.
                    facet.appended = True
                    # Filter out common values
                    facet.values -= common[facet.name]
                if facet.values:
                    request |= facet.dump()
            if request:
                requests.append(request)

        state = State(use=use, requests=requests)
        self._setdefault(full=True)
        self.load(state)


Facets.setup(DEFAULT_FACETS)


__all__ = ["Facets"]
