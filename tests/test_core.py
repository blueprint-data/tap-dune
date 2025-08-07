"""Tests for tap-dune."""

import json
from datetime import datetime
from typing import Dict, List

import pytest
import responses

from tap_dune.tap import TapDune


@pytest.fixture
def mock_config() -> Dict:
    """Mock tap config."""
    return {
        "api_key": "test_api_key",
        "query_id": "123456",
        "base_url": "https://api.dune.com/api/v1",
        "performance": "medium",
        "query_parameters": [
            {
                "key": "date_from",
                "value": "2025-08-01",
                "replication_key": True
            }
        ]
    }


@pytest.fixture
def mock_query_results() -> List[Dict]:
    """Mock query results with different data types."""
    return [
        {
            "day": "2025-08-01",
            "network": "ethereum",
            "total_mana": 1234.56,
            "total_usd": 5678.90,
            "is_active": True,
            "metadata": {"chain_id": 1},
            "tags": ["defi", "nft"],
            "timestamp": "2025-08-01T12:34:56.789Z",
            "null_field": None
        },
        {
            "day": "2025-08-02",
            "network": "polygon",
            "total_mana": 2345.67,
            "total_usd": 6789.01,
            "is_active": False,
            "metadata": {"chain_id": 137},
            "tags": ["gaming"],
            "timestamp": "2025-08-02T12:34:56.789Z",
            "null_field": "not_null"
        }
    ]


@pytest.fixture
def mock_api_responses(mock_query_results):
    """Mock Dune API responses."""
    rsps = responses.RequestsMock(assert_all_requests_are_fired=False)
    # Mock query execution
    rsps.add(
        responses.POST,
        "https://api.dune.com/api/v1/query/123456/execute",
        json={"execution_id": "exec123"},
        status=200,
    )

    # Mock status check
    rsps.add(
        responses.GET,
        "https://api.dune.com/api/v1/execution/exec123/status",
        json={"state": "QUERY_STATE_COMPLETED"},
        status=200,
    )

    # Mock results
    rsps.add(
        responses.GET,
        "https://api.dune.com/api/v1/execution/exec123/results",
        json={
            "result": {
                "rows": mock_query_results
            },
            "execution_ended_at": datetime.utcnow().isoformat()
        },
        status=200,
    )
    return rsps


def test_schema_inference(mock_config, mock_api_responses):
    """Test automatic schema inference from query results."""
    with mock_api_responses:
        tap = TapDune(config=mock_config)
        streams = list(tap.discover_streams())
        assert len(streams) == 1

        stream = streams[0]
        schema = stream.schema["properties"]

    # Test inferred types
    assert schema["day"]["type"] == "string"
    assert schema["day"]["format"] == "date"
    assert schema["network"]["type"] == "string"
    assert schema["total_mana"]["type"] == "number"
    assert schema["total_usd"]["type"] == "number"
    assert schema["is_active"]["type"] == "boolean"
    assert schema["metadata"]["type"] == "object"
    assert schema["tags"]["type"] == "array"
    assert schema["timestamp"]["type"] == "string"
    assert schema["timestamp"]["format"] == "date-time"
    assert schema["null_field"]["type"] == "string"  # Inferred from non-null value

    # Test execution metadata fields
    assert schema["execution_id"]["type"] == "string"
    assert schema["execution_time"]["type"] == "string"
    assert schema["execution_time"]["format"] == "date-time"


def test_explicit_schema(mock_config, mock_api_responses):
    """Test using explicitly defined schema."""
    explicit_schema = {
        "properties": {
            "custom_field": {"type": "string"},
            "another_field": {"type": "integer"}
        }
    }
    config_with_schema = {**mock_config, "schema": explicit_schema}
    
    with mock_api_responses:
        tap = TapDune(config=config_with_schema)
        streams = list(tap.discover_streams())
        assert len(streams) == 1

        stream = streams[0]
        schema = stream.schema["properties"]

        # Test explicit schema fields
        assert schema["custom_field"]["type"] == "string"
        assert schema["another_field"]["type"] == "integer"

        # Test that execution metadata fields are still present
        assert schema["execution_id"]["type"] == "string"
        assert schema["execution_time"]["type"] == "string"
        assert schema["execution_time"]["format"] == "date-time"


def test_replication_key(mock_config, mock_api_responses):
    """Test replication key configuration."""
    with mock_api_responses:
        tap = TapDune(config=mock_config)
        streams = list(tap.discover_streams())
        assert len(streams) == 1

        stream = streams[0]
        assert stream.replication_key == "date_from"


def test_query_parameters(mock_config, mock_api_responses):
    """Test query parameters are properly passed to API."""
    with mock_api_responses:
        tap = TapDune(config=mock_config)
        streams = list(tap.discover_streams())
        stream = streams[0]

        # Get the first record to trigger API call
        records = list(stream.get_records(None))
        
        # Check that the API was called with correct parameters
        assert len(mock_api_responses.calls) > 0
        execute_call = mock_api_responses.calls[0]
        request_body = json.loads(execute_call.request.body)
        
        assert request_body["performance"] == "medium"
        assert request_body["query_parameters"] == {"date_from": "2025-08-01"}