"""Test configuration for tap-dune."""

import pytest


@pytest.fixture(autouse=True)
def mock_sleep(monkeypatch):
    """Disable sleep in tests."""
    monkeypatch.setattr("time.sleep", lambda x: None)