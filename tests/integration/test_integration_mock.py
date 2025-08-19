from unittest.mock import Mock

import pytest


# --- Application code (same as your example) ---
class DatabaseClient:
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def get_user(self, user_id):
        raise NotImplementedError

    def save_user(self, user_data):
        raise NotImplementedError


class UserService:
    def __init__(self, db_client):
        self.db_client = db_client

    def get_user_details(self, user_id):
        user = self.db_client.get_user(user_id)
        if not user:
            return {"error": "User not found"}, 404
        return {"id": user["id"], "name": user["name"], "email": user["email"]}, 200

    def create_user(self, user_data):
        if not user_data.get("name") or not user_data.get("email"):
            return {"error": "Name and email are required"}, 400

        try:
            self.db_client.save_user(user_data)
            return {"message": "User created successfully", "id": user_data["id"]}, 201
        except Exception as e:
            return {"error": str(e)}, 500


# --- Pytest fixtures ---
@pytest.fixture
def mock_db_client():
    return Mock(spec=DatabaseClient)


@pytest.fixture
def user_service(mock_db_client):
    return UserService(mock_db_client)


# --- Tests ---
def test_get_user_details_success(user_service, mock_db_client):
    mock_db_client.get_user.return_value = {
        "id": 123,
        "name": "John Doe",
        "email": "john@example.com"
    }
    result, status_code = user_service.get_user_details(123)

    assert status_code == 200
    assert result["id"] == 123
    assert result["name"] == "John Doe"
    assert result["email"] == "john@example.com"
    mock_db_client.get_user.assert_called_once_with(123)


def test_get_user_details_not_found(user_service, mock_db_client):
    mock_db_client.get_user.return_value = None
    result, status_code = user_service.get_user_details(999)

    assert status_code == 404
    assert result["error"] == "User not found"
    mock_db_client.get_user.assert_called_once_with(999)


def test_create_user_success(user_service, mock_db_client):
    test_user = {
        "id": 456,
        "name": "Alice Smith",
        "email": "alice@example.com"
    }
    mock_db_client.save_user.return_value = None
    result, status_code = user_service.create_user(test_user)

    assert status_code == 201
    assert result["message"] == "User created successfully"
    assert result["id"] == 456
    mock_db_client.save_user.assert_called_once_with(test_user)


def test_create_user_missing_fields(user_service, mock_db_client):
    # Missing name
    result, status_code = user_service.create_user({
        "id": 789,
        "email": "bob@example.com"
    })
    assert status_code == 400
    assert result["error"] == "Name and email are required"

    # Missing email
    result, status_code = user_service.create_user({
        "id": 789,
        "name": "Bob Johnson"
    })
    assert status_code == 400
    assert result["error"] == "Name and email are required"

    mock_db_client.save_user.assert_not_called()


def test_create_user_database_error(user_service, mock_db_client):
    test_user = {
        "id": 789,
        "name": "Error Case",
        "email": "error@example.com"
    }
    mock_db_client.save_user.side_effect = Exception("Database error")

    result, status_code = user_service.create_user(test_user)

    assert status_code == 500
    assert result["error"] == "Database error"
    mock_db_client.save_user.assert_called_once_with(test_user)
