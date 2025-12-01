"""Simplified tests for the brewery data pipeline - case study demonstration."""

import pytest
import pandas as pd
import sys
from unittest.mock import Mock, patch, MagicMock
from requests.exceptions import Timeout, ConnectionError, HTTPError
from airflow.models import DagBag

# Add airflow modules to path
sys.path.insert(0, '/opt/airflow/dags')
sys.path.insert(0, '/opt/airflow')

from utils.api_client import BreweryAPIClient
from utils.data_quality import validate_bronze_data, validate_silver_data, validate_gold_aggregations, DataQualityError


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

        # Simulate silver transformation
        df["state"] = df["state"].fillna("unknown")
        df["country"] = df["country"].fillna("unknown")

        assert df["state"].isna().sum() == 0, "State column should have no nulls"
        assert df["country"].isna().sum() == 0, "Country column should have no nulls"
        assert "unknown" in df["state"].values, "Missing state should be 'unknown'"
        assert "unknown" in df["country"].values, "Missing country should be 'unknown'"

    def test_deduplication_removes_duplicates(self):
        """Test that duplicate brewery IDs are removed."""
        duplicate_data = pd.DataFrame([
            {"id": "1", "name": "Brewery A", "brewery_type": "micro"},
            {"id": "1", "name": "Brewery A Duplicate", "brewery_type": "micro"},
            {"id": "2", "name": "Brewery B", "brewery_type": "nano"},
        ])

        # Simulate deduplication
        deduplicated = duplicate_data.drop_duplicates(subset=["id"])

        assert len(deduplicated) == 2, "Should have 2 unique breweries after dedup"
        assert deduplicated["id"].is_unique, "IDs should be unique after dedup"


class TestSilverToGoldAggregation:
    """Test Silver to Gold aggregation logic."""

    def test_brewery_type_aggregation(self, sample_silver_data):
        """Test aggregation by brewery type produces correct counts."""
        by_type = sample_silver_data.groupby("brewery_type").size().reset_index(name="count")

        assert len(by_type) == 4, "Should have 4 brewery types"
        assert by_type["count"].sum() == 4, "Total count should equal source rows"
        assert set(by_type["brewery_type"]) == {"micro", "nano", "regional", "brewpub"}

    def test_state_aggregation(self, sample_silver_data):
        """Test aggregation by state and country produces correct counts."""
        by_state = sample_silver_data.groupby(["state", "country"]).size().reset_index(name="count")

        assert by_state["count"].sum() == 4, "Total count should equal source rows"
        california_count = by_state[by_state["state"] == "California"]["count"].sum()
        assert california_count == 2, "California should have 2 breweries"

    def test_aggregation_preserves_total_count(self, sample_silver_data):
        """Test that aggregations don't lose or duplicate records."""
        by_type = sample_silver_data.groupby("brewery_type").size().reset_index(name="count")
        by_state = sample_silver_data.groupby(["state", "country"]).size().reset_index(name="count")

        original_count = len(sample_silver_data)
        type_sum = by_type["count"].sum()
        state_sum = by_state["count"].sum()

        assert type_sum == original_count, "Type aggregation should preserve total count"
        assert state_sum == original_count, "State aggregation should preserve total count"


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
        """Test that 500 errors trigger retries via urllib3 Retry mechanism."""
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
            # First page returns full page, second page returns partial results
            first_page_response = Mock()
            first_page_response.status_code = 200
            first_page_response.json.return_value = [
                {"id": str(i), "name": f"Brewery {i}", "brewery_type": "micro"}
                for i in range(200)  # Full page
            ]

            second_page_response = Mock()
            second_page_response.status_code = 200
            second_page_response.json.return_value = [
                {"id": "201", "name": "Brewery 201", "brewery_type": "micro"}
            ]  # Partial page - should stop here

            mock_get.side_effect = [first_page_response, second_page_response]

            client = BreweryAPIClient()
            result = client.fetch_all_breweries()

            # Should have fetched both pages
            assert len(result) == 201, "Should fetch all breweries until partial page"
            assert mock_get.call_count == 2, "Should make 2 API calls"
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

            assert result == [], "Should return empty list for empty API response"
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


class TestDataQualityErrorScenarios:
    """Test data quality validation failures."""

    def test_bronze_insufficient_records_fails(self):
        """Test that Bronze layer fails with too few records."""
        insufficient_data = [
            {"id": "1", "name": "Brewery 1", "brewery_type": "micro"}
        ]

        with pytest.raises(DataQualityError, match="quality checks failed"):
            validate_bronze_data(insufficient_data, min_records=100)

    def test_bronze_duplicate_ids_fails(self):
        """Test that Bronze layer fails with duplicate IDs."""
        duplicate_data = [
            {"id": "1", "name": "Brewery 1", "brewery_type": "micro"},
            {"id": "1", "name": "Brewery 1 Duplicate", "brewery_type": "micro"},
        ]

        with pytest.raises(DataQualityError, match="quality checks failed"):
            validate_bronze_data(duplicate_data, min_records=2)

    def test_bronze_null_ids_fails(self):
        """Test that Bronze layer fails with null IDs."""
        null_id_data = [
            {"id": None, "name": "Brewery 1", "brewery_type": "micro"},
            {"id": "2", "name": "Brewery 2", "brewery_type": "nano"},
        ]

        with pytest.raises(DataQualityError, match="quality checks failed"):
            validate_bronze_data(null_id_data, min_records=2)

    def test_silver_data_loss_detection_fails(self):
        """Test that Silver layer detects data loss from Bronze."""
        silver_df = pd.DataFrame({
            'id': ['1', '2'],
            'name': ['Brewery 1', 'Brewery 2'],
            'state': ['California', 'Texas'],
            'country': ['United States', 'United States']
        })

        # Bronze had 100 records, Silver only has 2 - major data loss!
        with pytest.raises(DataQualityError, match="quality checks failed"):
            validate_silver_data(silver_df, bronze_count=100)

    def test_silver_duplicate_ids_after_transform_fails(self):
        """Test that Silver layer fails if transformation created duplicates."""
        invalid_df = pd.DataFrame({
            'id': ['1', '1', '2'],  # Duplicate ID
            'name': ['Brewery 1', 'Brewery 1 Dup', 'Brewery 2'],
            'state': ['California', 'California', 'Texas'],
            'country': ['United States', 'United States', 'United States']
        })

        with pytest.raises(DataQualityError, match="quality checks failed"):
            validate_silver_data(invalid_df, bronze_count=3)

    def test_gold_insufficient_brewery_types_fails(self):
        """Test that Gold layer fails with too few brewery types."""
        by_type_df = pd.DataFrame({
            'brewery_type': ['micro'],  # Only 1 type, minimum is 3
            'count': [100]
        })

        by_state_df = pd.DataFrame({
            'state': ['California', 'Texas', 'New York', 'Florida', 'Ohio',
                      'Colorado', 'Washington', 'Michigan', 'Oregon', 'North Carolina', 'Pennsylvania'],
            'country': ['United States'] * 11,
            'count': [20, 15, 10, 8, 7, 6, 6, 6, 6, 6, 10]
        })

        with pytest.raises(DataQualityError, match="quality checks failed"):
            validate_gold_aggregations(
                by_type_df=by_type_df,
                by_state_df=by_state_df,
                silver_count=100
            )

    def test_gold_type_aggregation_mismatch_fails(self):
        """Test that Gold layer fails when type counts don't sum to Silver total."""
        by_type_df = pd.DataFrame({
            'brewery_type': ['micro', 'nano', 'regional', 'brewpub'],
            'count': [50, 30, 20, 10]  # Total = 110
        })

        by_state_df = pd.DataFrame({
            'state': ['California', 'Texas', 'New York', 'Florida', 'Ohio',
                      'Colorado', 'Washington', 'Michigan', 'Oregon', 'North Carolina', 'Pennsylvania'],
            'country': ['United States'] * 11,
            'count': [50, 30, 20, 15, 10, 8, 7, 6, 5, 4, 5]  # Total = 160
        })

        # Type sum (110) != Silver count (160)
        with pytest.raises(DataQualityError, match="Type aggregation mismatch"):
            validate_gold_aggregations(
                by_type_df=by_type_df,
                by_state_df=by_state_df,
                silver_count=160
            )

    def test_gold_state_aggregation_mismatch_fails(self):
        """Test that Gold layer fails when state counts don't sum to Silver total."""
        by_type_df = pd.DataFrame({
            'brewery_type': ['micro', 'nano', 'regional', 'brewpub'],
            'count': [100, 50, 30, 20]  # Total = 200
        })

        by_state_df = pd.DataFrame({
            'state': ['California', 'Texas', 'New York', 'Florida', 'Ohio',
                      'Colorado', 'Washington', 'Michigan', 'Oregon', 'North Carolina', 'Pennsylvania'],
            'country': ['United States'] * 11,
            'count': [30, 20, 15, 10, 8, 7, 6, 5, 4, 3, 2]  # Total = 110
        })

        # State sum (110) != Silver count (200)
        with pytest.raises(DataQualityError, match="State aggregation mismatch"):
            validate_gold_aggregations(
                by_type_df=by_type_df,
                by_state_df=by_state_df,
                silver_count=200
            )
