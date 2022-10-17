ENV_VARNAME = "ESGPULL_HOME"

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
    "member_id",
    "cmor_table",
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
    "experiment_family",
]
DEFAULT_CONSTRAINTS_WITH_VALUE: dict[str, str] = {}
