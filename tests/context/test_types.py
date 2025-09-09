import pytest

from esgpull.context.types import IndexNode
from esgpull.models import ApiBackend
from tests.utils import CEDA_NODE, CEDA_STAC, IPSL_NODE, ORNL_BRIDGE


@pytest.mark.parametrize(
    "index_node,expected_url,is_bridge",
    [
        (
            IndexNode(
                backend=ApiBackend.solr,
                value=IPSL_NODE,
            ),
            f"https://{IPSL_NODE}/esg-search/search",
            False,
        ),
        (
            IndexNode(
                backend=ApiBackend.solr,
                value=CEDA_NODE,
            ),
            f"https://{CEDA_NODE}/esg-search/search",
            False,
        ),
        (
            IndexNode(
                backend=ApiBackend.solr,
                value=f"https://{IPSL_NODE}/esg-search/search",
            ),
            f"https://{IPSL_NODE}/esg-search/search",
            False,
        ),
        (
            IndexNode(
                backend=ApiBackend.solr,
                value=f"https://{CEDA_NODE}/esg-search/search",
            ),
            f"https://{CEDA_NODE}/esg-search/search",
            False,
        ),
        (
            IndexNode(
                backend=ApiBackend.solr,
                value=ORNL_BRIDGE,
            ),
            f"https://{ORNL_BRIDGE}",
            True,
        ),
        (
            IndexNode(
                backend=ApiBackend.solr,
                value=f"https://{ORNL_BRIDGE}",
            ),
            f"https://{ORNL_BRIDGE}",
            True,
        ),
        (
            IndexNode(
                backend=ApiBackend.stac,
                value=CEDA_STAC,
            ),
            f"https://{CEDA_STAC}",
            False,
        ),
        (
            IndexNode(
                backend=ApiBackend.stac,
                value=f"https://{CEDA_STAC}",
            ),
            f"https://{CEDA_STAC}",
            False,
        ),
    ],
)
def test_index_node(index_node: IndexNode, expected_url: str, is_bridge: bool):
    assert index_node.url == expected_url
    assert index_node.is_bridge() == is_bridge
