DefaultEsgfUrl: str = "http://esgf-node.ipsl.upmc.fr/esg-search"

# TODO: fetch these from database table `param`
DefaultConstraints: list[str] = [
    "mip_era",
    "variable_id",
    "institution_id",
]
DefaultConstraintsWithValue: dict[str, str] = {"type": "Dataset"}
