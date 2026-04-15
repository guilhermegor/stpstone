"""Integration tests demonstrating mock-based service/repository patterns."""

from typing import Any
from unittest.mock import Mock

import pytest


class DatabaseClient:
	"""Minimal database client interface for integration tests."""

	def __init__(self, connection_string: str) -> None:
		"""Initialise client with a connection string.

		Parameters
		----------
		connection_string : str
			Database connection URI.
		"""
		self.connection_string = connection_string

	def get_user(self, user_id: int) -> dict[str, Any]:
		"""Return user record by ID.

		Parameters
		----------
		user_id : int
			Primary key of the user.

		Returns
		-------
		dict[str, Any]
			User record, or raises NotImplementedError if not implemented.
		"""
		raise NotImplementedError

	def save_user(self, user_data: dict[str, Any]) -> None:
		"""Persist a user record.

		Parameters
		----------
		user_data : dict[str, Any]
			User fields to persist.

		Returns
		-------
		None
		"""
		raise NotImplementedError


class UserService:
	"""Application service layer for user operations."""

	def __init__(self, db_client: DatabaseClient) -> None:
		"""Initialise service with an injected database client.

		Parameters
		----------
		db_client : DatabaseClient
			Repository used for persistence.
		"""
		self.db_client = db_client

	def get_user_details(self, user_id: int) -> tuple[dict[str, Any], int]:
		"""Return user details and an HTTP-style status code.

		Parameters
		----------
		user_id : int
			Primary key of the user.

		Returns
		-------
		tuple[dict[str, Any], int]
			Payload dict and status code (200 on success, 404 if not found).
		"""
		user = self.db_client.get_user(user_id)
		if not user:
			return {"error": "User not found"}, 404
		return {"id": user["id"], "name": user["name"], "email": user["email"]}, 200

	def create_user(self, user_data: dict[str, Any]) -> tuple[dict[str, Any], int]:
		"""Create a new user and return a response payload and status code.

		Parameters
		----------
		user_data : dict[str, Any]
			Fields required: 'name', 'email', 'id'.

		Returns
		-------
		tuple[dict[str, Any], int]
			Payload dict and status code (201 on success, 400 on bad input,
			500 on database error).
		"""
		if not user_data.get("name") or not user_data.get("email"):
			return {"error": "Name and email are required"}, 400

		try:
			self.db_client.save_user(user_data)
			return {"message": "User created successfully", "id": user_data["id"]}, 201
		except Exception as e:
			return {"error": str(e)}, 500


# --- Pytest fixtures ---
@pytest.fixture
def mock_db_client() -> Mock:
	"""Return a Mock scoped to DatabaseClient.

	Returns
	-------
	Mock
		Spec-constrained mock of DatabaseClient.
	"""
	return Mock(spec=DatabaseClient)


@pytest.fixture
def user_service(mock_db_client: Mock) -> UserService:
	"""Return a UserService wired with a mock database client.

	Parameters
	----------
	mock_db_client : Mock
		Injected mock database client.

	Returns
	-------
	UserService
		Service instance under test.
	"""
	return UserService(mock_db_client)


# --- Tests ---
def test_get_user_details_success(user_service: UserService, mock_db_client: Mock) -> None:
	"""Test get_user_details returns 200 and correct payload for a found user.

	Parameters
	----------
	user_service : UserService
		Service instance under test.
	mock_db_client : Mock
		Mock repository to configure return values.
	"""
	mock_db_client.get_user.return_value = {
		"id": 123,
		"name": "John Doe",
		"email": "john@example.com",
	}
	result, status_code = user_service.get_user_details(123)

	assert status_code == 200
	assert result["id"] == 123
	assert result["name"] == "John Doe"
	assert result["email"] == "john@example.com"
	mock_db_client.get_user.assert_called_once_with(123)


def test_get_user_details_not_found(user_service: UserService, mock_db_client: Mock) -> None:
	"""Test get_user_details returns 404 when the user does not exist.

	Parameters
	----------
	user_service : UserService
		Service instance under test.
	mock_db_client : Mock
		Mock repository returning None for the queried user.
	"""
	mock_db_client.get_user.return_value = None
	result, status_code = user_service.get_user_details(999)

	assert status_code == 404
	assert result["error"] == "User not found"
	mock_db_client.get_user.assert_called_once_with(999)


def test_create_user_success(user_service: UserService, mock_db_client: Mock) -> None:
	"""Test create_user returns 201 and confirmation payload on success.

	Parameters
	----------
	user_service : UserService
		Service instance under test.
	mock_db_client : Mock
		Mock repository confirming successful save.
	"""
	test_user = {"id": 456, "name": "Alice Smith", "email": "alice@example.com"}
	mock_db_client.save_user.return_value = None
	result, status_code = user_service.create_user(test_user)

	assert status_code == 201
	assert result["message"] == "User created successfully"
	assert result["id"] == 456
	mock_db_client.save_user.assert_called_once_with(test_user)


def test_create_user_missing_fields(user_service: UserService, mock_db_client: Mock) -> None:
	"""Test create_user returns 400 when required fields are absent.

	Parameters
	----------
	user_service : UserService
		Service instance under test.
	mock_db_client : Mock
		Mock repository that must not be called on validation failure.
	"""
	result, status_code = user_service.create_user({"id": 789, "email": "bob@example.com"})
	assert status_code == 400
	assert result["error"] == "Name and email are required"

	result, status_code = user_service.create_user({"id": 789, "name": "Bob Johnson"})
	assert status_code == 400
	assert result["error"] == "Name and email are required"

	mock_db_client.save_user.assert_not_called()


def test_create_user_database_error(user_service: UserService, mock_db_client: Mock) -> None:
	"""Test create_user returns 500 when the database raises an exception.

	Parameters
	----------
	user_service : UserService
		Service instance under test.
	mock_db_client : Mock
		Mock repository configured to raise on save.
	"""
	test_user = {"id": 789, "name": "Error Case", "email": "error@example.com"}
	mock_db_client.save_user.side_effect = Exception("Database error")

	result, status_code = user_service.create_user(test_user)

	assert status_code == 500
	assert result["error"] == "Database error"
	mock_db_client.save_user.assert_called_once_with(test_user)
