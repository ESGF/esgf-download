from esgpull.db import Database

# Open a `:memory:` sqlite database
db = Database(path="~/ipsl/synda/data/db/sdt_new.db", verbosity=1)
print(db.version)
