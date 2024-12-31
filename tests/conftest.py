import pytest
from fastapi.testclient import TestClient

from simcc import app


@pytest.fixture
def client():
    return TestClient(app)
