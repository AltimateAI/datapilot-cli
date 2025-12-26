import pytest

from datapilot.core.platforms.dbt.utils import load_run_results
from datapilot.core.platforms.dbt.utils import load_sources
from datapilot.exceptions.exceptions import AltimateFileNotFoundError


class TestLoadRunResults:
    def test_load_run_results_v6(self):
        run_results_path = "tests/data/run_results_v6.json"
        run_results = load_run_results(run_results_path)

        assert run_results is not None
        assert run_results.metadata.dbt_schema_version == "https://schemas.getdbt.com/dbt/run-results/v6.json"
        assert len(run_results.results) == 1
        assert run_results.results[0].status.value == "success"
        assert run_results.results[0].unique_id == "model.jaffle_shop.stg_customers"

    def test_load_run_results_file_not_found(self):
        with pytest.raises(AltimateFileNotFoundError):
            load_run_results("nonexistent_file.json")


class TestLoadSources:
    def test_load_sources_v3(self):
        sources_path = "tests/data/sources_v3.json"
        sources = load_sources(sources_path)

        assert sources is not None
        assert sources.metadata.dbt_schema_version == "https://schemas.getdbt.com/dbt/sources/v3.json"
        assert len(sources.results) == 1
        assert sources.results[0].unique_id == "source.jaffle_shop.raw.customers"

    def test_load_sources_file_not_found(self):
        with pytest.raises(AltimateFileNotFoundError):
            load_sources("nonexistent_file.json")
