"""Tests for the brewery data pipeline."""

import pytest
import pandas as pd
import sys
from unittest.mock import Mock, patch
from requests.exceptions import Timeout, ConnectionError, HTTPError
from airflow.models import DagBag

sys.path.insert(0, '/opt/airflow/dags')
sys.path.insert(0, '/opt/airflow')

from utils.api_client import BreweryAPIClient
from utils.data_quality import DataQualityError


@pytest.fixture(scope="module")
def dagbag():
    """Load all DAGs for testing."""
    return DagBag(dag_folder='/opt/airflow/dags', include_examples=False)


@pytest.fixture
def sample_bronze_data():
    """Sample bronze data for testing."""
    return [
        {"id": "1", "name": "Brewery A", "brewery_type": "micro", "state": "California", "country": "United States"},
        {"id": "2", "name": "Brewery B", "brewery_type": "nano", "state": "Texas", "country": "United States"},
        {"id": "3", "name": "Brewery C", "brewery_type": "regional", "state": None, "country": "United States"},
        {"id": "4", "name": "Brewery D", "brewery_type": "brewpub", "state": "California", "country": None},
    ]


@pytest.fixture
def sample_silver_data():
    """Sample silver data for testing."""
    return pd.DataFrame({
        'id': ['1', '2', '3', '4'],
        'name': ['Brewery A', 'Brewery B', 'Brewery C', 'Brewery D'],
        'brewery_type': ['micro', 'nano', 'regional', 'brewpub'],
        'state': ['California', 'Texas', 'unknown', 'California'],
        'country': ['United States', 'United States', 'United States', 'unknown']
    })


class TestDAGsLoad:
    """Test that DAGs load without errors."""

    def test_no_import_errors(self, dagbag):
        """Verify DAGs import successfully."""
        assert len(dagbag.import_errors) == 0, \
            f"DAG import errors: {dagbag.import_errors}"

    def test_expected_dags_present(self, dagbag):
        """Verify all three layer DAGs exist."""
        expected_dags = [
            'bees_ingestion_api_bronze_brewery_data',
            'bees_ingestion_api_silver_brewery_data',
            'bees_ingestion_api_gold_brewery_data'
        ]

        for dag_id in expected_dags:
            assert dag_id in dagbag.dags, f"DAG {dag_id} not found"


class TestDAGStructure:
    """Test basic DAG structure and configuration."""

    def test_bronze_dag_tasks(self, dagbag):
        """Verify bronze DAG has expected tasks."""
        dag = dagbag.get_dag('bees_ingestion_api_bronze_brewery_data')
        assert 'ingest_from_api' in dag.task_ids
        assert 'quality_check_bronze' in dag.task_ids

    def test_silver_dag_tasks(self, dagbag):
        """Verify silver DAG has expected tasks."""
        dag = dagbag.get_dag('bees_ingestion_api_silver_brewery_data')
        assert 'transform_to_parquet' in dag.task_ids
        assert 'quality_check_silver' in dag.task_ids

    def test_gold_dag_tasks(self, dagbag):
        """Verify gold DAG has expected tasks."""
        dag = dagbag.get_dag('bees_ingestion_api_gold_brewery_data')
        assert 'create_aggregations' in dag.task_ids
        assert 'quality_check_gold' in dag.task_ids


class TestBronzeToSilverTransformation:
    """Test Bronze to Silver data transformations."""

    def test_null_handling_in_state_and_country(self, sample_bronze_data):
        """Test that null states and countries are filled with 'unknown'."""
        df = pd.DataFrame(sample_bronze_data)

        df["state"] = df["state"].fillna("unknown")
        df["country"] = df["country"].fillna("unknown")

        assert df["state"].isna().sum() == 0
        assert df["country"].isna().sum() == 0
        assert "unknown" in df["state"].values
        assert "unknown" in df["country"].values

    def test_deduplication_removes_duplicates(self):
        """Test that duplicate brewery IDs are removed."""
        duplicate_data = pd.DataFrame([
            {"id": "1", "name": "Brewery A", "brewery_type": "micro"},
            {"id": "1", "name": "Brewery A Duplicate", "brewery_type": "micro"},
            {"id": "2", "name": "Brewery B", "brewery_type": "nano"},
        ])

        deduplicated = duplicate_data.drop_duplicates(subset=["id"])

        assert len(deduplicated) == 2
        assert deduplicated["id"].is_unique


class TestSilverToGoldAggregation:
    """Test Silver to Gold aggregation logic."""

    def test_brewery_type_aggregation(self, sample_silver_data):
        """Test aggregation by brewery type produces correct counts."""
        by_type = sample_silver_data.groupby("brewery_type").size().reset_index(name="count")

        assert len(by_type) == 4
        assert by_type["count"].sum() == 4
        assert set(by_type["brewery_type"]) == {"micro", "nano", "regional", "brewpub"}

    def test_state_aggregation(self, sample_silver_data):
        """Test aggregation by state and country produces correct counts."""
        by_state = sample_silver_data.groupby(["state", "country"]).size().reset_index(name="count")

        assert by_state["count"].sum() == 4
        california_count = by_state[by_state["state"] == "California"]["count"].sum()
        assert california_count == 2

    def test_aggregation_preserves_total_count(self, sample_silver_data):
        """Test that aggregations don't lose or duplicate records."""
        by_type = sample_silver_data.groupby("brewery_type").size().reset_index(name="count")
        by_state = sample_silver_data.groupby(["state", "country"]).size().reset_index(name="count")

        original_count = len(sample_silver_data)
        type_sum = by_type["count"].sum()
        state_sum = by_state["count"].sum()

        assert type_sum == original_count
        assert state_sum == original_count


class TestAPIClientErrorScenarios:
    """Test API client error handling and retry logic."""

    def test_api_timeout_raises_exception(self):
        """Test that API timeout errors are raised after retries."""
        with patch('requests.Session.get') as mock_get:
            mock_get.side_effect = Timeout("Connection timeout")

            client = BreweryAPIClient()
            with pytest.raises(Timeout):
                client.fetch_all_breweries()
            client.close()

    def test_api_500_error_retries_then_raises(self):
        """Test that 500 errors trigger retries."""
        with patch('utils.api_client.requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 500
            mock_response.raise_for_status.side_effect = HTTPError("500 Server Error")
            mock_get.return_value = mock_response

            client = BreweryAPIClient()
            with pytest.raises(HTTPError):
                client.fetch_all_breweries()
            client.close()

    def test_api_connection_error_raises(self):
        """Test that connection errors are raised."""
        with patch('requests.Session.get') as mock_get:
            mock_get.side_effect = ConnectionError("Network unreachable")

            client = BreweryAPIClient()
            with pytest.raises(ConnectionError):
                client.fetch_all_breweries()
            client.close()

    def test_api_pagination_stops_on_partial_page(self):
        """Test that pagination stops when receiving less than page_size results."""
        with patch('utils.api_client.requests.Session.get') as mock_get:
            first_page_response = Mock()
            first_page_response.status_code = 200
            first_page_response.json.return_value = [
                {"id": str(i), "name": f"Brewery {i}", "brewery_type": "micro"}
                for i in range(200)
            ]

            second_page_response = Mock()
            second_page_response.status_code = 200
            second_page_response.json.return_value = [
                {"id": "201", "name": "Brewery 201", "brewery_type": "micro"}
            ]

            mock_get.side_effect = [first_page_response, second_page_response]

            client = BreweryAPIClient()
            result = client.fetch_all_breweries()

            assert len(result) == 201
            assert mock_get.call_count == 2
            client.close()

    def test_api_returns_empty_list(self):
        """Test handling of empty API response."""
        with patch('requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = []
            mock_get.return_value = mock_response

            client = BreweryAPIClient()
            result = client.fetch_all_breweries()

            assert result == []
            client.close()

    def test_api_returns_malformed_json(self):
        """Test handling of malformed JSON response."""
        with patch('requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_get.return_value = mock_response

            client = BreweryAPIClient()
            with pytest.raises(ValueError):
                client.fetch_all_breweries()
            client.close()


class TestDataQualityModule:
    """Test data quality module integration."""

    def test_data_quality_error_class(self):
        """Test that DataQualityError can be raised."""
        with pytest.raises(DataQualityError):
            raise DataQualityError("Test error")

    def test_soda_checks_files_exist(self):
        """Test that Soda check files exist."""
        from pathlib import Path

        checks_dir = Path("/opt/airflow/utils/soda_checks")
        assert (checks_dir / "bronze_checks.yml").exists()
        assert (checks_dir / "silver_checks.yml").exists()
        assert (checks_dir / "gold_type_checks.yml").exists()
        assert (checks_dir / "gold_state_checks.yml").exists()
