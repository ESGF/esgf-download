from esgpull import Storage, StorageMode, Semver

# Open a `:memory:` sqlite database
storage = Storage(
    mode=StorageMode.Sqlite, path=None, verbosity=1, semver=Semver(4, 0)
)
print(storage.semver)
