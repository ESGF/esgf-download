from esgpull.db.models import File


class Result:
    ok: bool
    file: File
    completed: int
    err: BaseException | None


class Ok(Result):
    ok = True
    file: File
    completed: int
    err = None

    def __init__(self, file: File, completed: int) -> None:
        self.file = file
        self.completed = completed


class Err(Result):
    ok = False
    file: File
    completed: int
    err: BaseException

    def __init__(self, file: File, completed: int, err: BaseException) -> None:
        self.file = file
        self.completed = completed
        self.err = err
