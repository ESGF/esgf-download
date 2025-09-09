from typing import Any, Mapping

IPSL_NODE = "esgf-node.ipsl.upmc.fr"
DRKZ_NODE = "esgf-data.dkrz.de"
CEDA_NODE = "esgf.ceda.ac.uk"
ORNL_BRIDGE = "esgf-node.ornl.gov/esgf-1-5-bridge"
CEDA_STAC = "api.stac.esgf.ceda.ac.uk"


def dict_equals_ignore(
    d1: Mapping[str, Any],
    d2: Mapping[str, Any],
    ignore_keys: list[str],
) -> bool:
    d1 = {k: v for k, v in d1.items() if k not in ignore_keys}
    d2 = {k: v for k, v in d2.items() if k not in ignore_keys}
    return d1 == d2
