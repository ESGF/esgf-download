from pyesgf.search import SearchConnection  # , SearchContext

from esgpull.context.facet import Facets, Constraints
from esgpull.context.constants import DefaultEsgfUrl


# TODO: add url/storage query builder from this
class Context:
    def __init__(self, lazy_update: bool = True):
        self.lazy_update = lazy_update
        self._conn = SearchConnection(DefaultEsgfUrl, distrib=False)
        self._ctx = self._conn.new_context()
        self._facets = Facets()

    @property
    def facets(self) -> Facets:
        return self._facets

    def get_constraints(self) -> Constraints:
        return Constraints(self.facets)

    def __repr__(self) -> str:
        return (
            f"Context(lazy_update={self.lazy_update}, "
            f"constraints={self.get_constraints()})"
        )


if __name__ == "__main__":
    # TODO: use these as unit tests
    c1 = Context()
    print("c1:", c1)
    print("c1.facets:", c1.facets)
    print("list(c1.facets):", list(c1.facets))
    print()

    c2 = Context()
    c1.facets["variable_id"] = "toto"
    c2.facets.variable_id = "tutu"

    try:
        c1.facets["variable"] = "toto"
    except Exception as e:
        print(e)

    print("c1.facets.variable_id:", c1.facets.variable_id)
    print("c2.facets.variable_id:", c2.facets.variable_id)
    print()
    print("c1.facets:", c1.facets)
    print("c2.facets:", c2.facets)
    print()
    print("c1.get_constraints():", c1.get_constraints())
    print("c2.get_constraints():", c2.get_constraints())
    print()
    print(c1.facets.__dict__)
    # print(c2.__dict__)
