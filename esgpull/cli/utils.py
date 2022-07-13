import click
from click_params import ListParamType


class SliceParam(ListParamType):
    name = "slice"

    def __init__(self, separator: str = "-") -> None:
        super().__init__(click.INT, separator=separator, name="integers")

    def convert(self, value: str, param, ctx) -> slice:
        converted_list = super().convert(value, param, ctx)
        result: slice
        match converted_list:
            case [stop]:
                result = slice(0, stop)
            case [start, stop]:
                result = slice(start, stop)
            case _:
                self.fail(
                    self._error_message.format(errors="Bad value"), param, ctx
                )
        return result
