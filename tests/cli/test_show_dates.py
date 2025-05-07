from datetime import datetime, timezone

from click.testing import CliRunner

from esgpull import Esgpull
from esgpull.cli.self import install
from esgpull.cli.show import show
from esgpull.install_config import InstallConfig
from esgpull.models import Query


def test_show_date_filters(tmp_path):
    """Test the --after and --before date filters for the show command."""
    # Setup environment
    InstallConfig.setup(tmp_path)
    install_path = tmp_path / "esgpull"
    runner = CliRunner()
    result_install = runner.invoke(install, [f"{install_path}"])
    assert result_install.exit_code == 0

    # Initialize Esgpull
    esg = Esgpull(install_path)
    
    # Create two queries with different dates
    old_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
    new_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    
    # Query 1 - older date
    query1 = Query(selection=dict(project="CMIP5", variable_id="tas"))
    query1.added_at = old_date
    query1.updated_at = old_date
    query1.compute_sha()
    
    # Query 2 - newer date
    query2 = Query(selection=dict(project="CMIP6", variable_id="pr"))
    query2.added_at = new_date
    query2.updated_at = new_date
    query2.compute_sha()
    
    # Add queries to the database
    esg.graph.add(query1)
    esg.graph.add(query2)
    esg.graph.merge()
    
    # Test the --after filter (show only newer query)
    result_after = runner.invoke(show, ["--after", "2022-01-01", "--shas"])
    assert result_after.exit_code == 0
    assert query2.sha in result_after.output
    assert query1.sha not in result_after.output
    
    # Test the --before filter (show only older query)
    result_before = runner.invoke(show, ["--before", "2022-01-01", "--shas"])
    assert result_before.exit_code == 0
    assert query1.sha in result_before.output
    assert query2.sha not in result_before.output
    
    # Test using both filters together (no results in this range)
    result_range = runner.invoke(show, ["--after", "2021-01-01", "--before", "2022-01-01", "--shas"])
    assert result_range.exit_code == 0
    assert query1.sha not in result_range.output
    assert query2.sha not in result_range.output