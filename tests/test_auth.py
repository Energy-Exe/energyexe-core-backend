"""Tests for authentication endpoints."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def user_data():
    """Sample user data for testing."""
    return {
        "email": "test@example.com",
        "username": "testuser",
        "password": "testpassword123",
        "first_name": "Test",
        "last_name": "User",
    }


def test_register_user(client: TestClient, user_data):
    """Test user registration."""
    response = client.post("/api/v1/auth/register", json=user_data)
    assert response.status_code == 201
    data = response.json()
    assert data["email"] == user_data["email"]
    assert data["username"] == user_data["username"]
    assert "hashed_password" not in data  # Security check


def test_register_duplicate_email(client: TestClient, user_data):
    """Test registration with duplicate email fails."""
    # Register first user
    client.post("/api/v1/auth/register", json=user_data)

    # Try to register another user with same email
    user_data_2 = user_data.copy()
    user_data_2["username"] = "testuser2"
    response = client.post("/api/v1/auth/register", json=user_data_2)
    assert response.status_code == 400
    assert "email" in response.json()["error"]["message"].lower()


def test_register_duplicate_username(client: TestClient, user_data):
    """Test registration with duplicate username fails."""
    # Register first user
    client.post("/api/v1/auth/register", json=user_data)

    # Try to register another user with same username
    user_data_2 = user_data.copy()
    user_data_2["email"] = "test2@example.com"
    response = client.post("/api/v1/auth/register", json=user_data_2)
    assert response.status_code == 400
    assert "username" in response.json()["error"]["message"].lower()


def test_login_success(client: TestClient, user_data):
    """Test successful login."""
    # Register user first
    register_response = client.post("/api/v1/auth/register", json=user_data)
    assert register_response.status_code == 201

    # Login with JSON data
    login_data = {
        "username": user_data["username"],
        "password": user_data["password"],
    }
    response = client.post("/api/v1/auth/login", json=login_data)
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


def test_login_invalid_credentials(client: TestClient, user_data):
    """Test login with invalid credentials."""
    # Register user first
    client.post("/api/v1/auth/register", json=user_data)

    # Try login with wrong password
    login_data = {
        "username": user_data["username"],
        "password": "wrongpassword",
    }
    response = client.post("/api/v1/auth/login", json=login_data)
    assert response.status_code == 401
    assert "incorrect" in response.json()["error"]["message"].lower()


def test_login_nonexistent_user(client: TestClient):
    """Test login with non-existent user."""
    login_data = {
        "username": "nonexistent",
        "password": "password",
    }
    response = client.post("/api/v1/auth/login", json=login_data)
    assert response.status_code == 401
    assert "incorrect" in response.json()["error"]["message"].lower()
