DEFAULT_ESGF_URL: str = "https://esgf-node.ipsl.upmc.fr/esg-search/search"
DEFAULT_ESGF_INDEX: str = "esgf-node.ipsl.upmc.fr"

# DEFAULT_ESGF_INDEX: str = "esgf-data.dkrz.de"


DEFAULT_FACETS = [
    "project",
    "mip_era",
    "experiment",
    "experiment_id",
    "institute",
    "institution_id",
    "model",
    "table_id",
    "activity_id",
    "ensemble",
    "variant_label",
    "realm",
    "frequency",
    "time_frequency",
    "variable",
    "variable_id",
    "dataset_id",
    "source_id",
    "domain",
    "driving_model",
    "rcm_name",
]
EXTRA_FACETS = [
    "start",
    "end",
    "query",
    # "fields",
    "facets",
    "url",
    "data_node",
    "index_node",
    "instance_id",  # search does not work with instance_id
    "title",
    "variable_long_name",
]
DEFAULT_CONSTRAINTS_WITH_VALUE: dict[str, str] = {}

DOWNLOAD_CHUNK_SIZE: int = 1 << 26  # 64.0 MiB
