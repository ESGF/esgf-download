CONFIG_FILENAME = "config.toml"
INSTALLS_PATH_ENV = "ESGPULL_INSTALLS_PATH"
ROOT_ENV = "ESGPULL_CURRENT"

IDP = "/esgf-idp/openid/"
CEDA_IDP = "/OpenID/Provider/server/"
PROVIDERS = {
    "esg-dn1.nsc.liu.se": IDP,
    "esgf-data.dkrz.de": IDP,
    "ceda.ac.uk": CEDA_IDP,
    "esgf-node.ipsl.upmc.fr": IDP,
    "esgf-node.llnl.gov": IDP,
    "esgf.nci.org.au": IDP,
}


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
    "query",
    "start",
    "end",
    # "fields",
    "facets",
    "url",
    "data_node",
    "index_node",
    "master_id",
    "instance_id",  # search does not work with instance_id
    "title",
    "variable_long_name",
    "experiment_family",
]
DEFAULT_CONSTRAINTS_WITH_VALUE: dict[str, str] = {}
