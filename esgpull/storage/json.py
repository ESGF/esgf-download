from pathlib import Path


# TODO: this should be placeholder for now..
class JsonStorage:
    def __init__(self, json_path: Path):
        self.json_path = json_path
        if not self.exists():
            self.create()

    def exists(self):
        return False

    def create(self):
        print("coucou")
