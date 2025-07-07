import unittest
from unittest.mock import Mock


# this would be the actual database client
class DatabaseClient:
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def get_user(self, user_id):
        # in a real implementation, this would query a database
        raise NotImplementedError

    def save_user(self, user_data):
        # in a real implementation, this would save to a database
        raise NotImplementedError

# this would the actual API service in a real application
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


class TestUserServiceIntegration(unittest.TestCase):
    def setUp(self):
        self.mock_db_client = Mock(spec=DatabaseClient)
        self.user_service = UserService(self.mock_db_client)

    def test_get_user_details_success(self):
        self.mock_db_client.get_user.return_value = {
            "id": 123,
            "name": "John Doe",
            "email": "john@example.com"
        }
        result, status_code = self.user_service.get_user_details(123)
        self.assertEqual(status_code, 200)
        self.assertEqual(result["id"], 123)
        self.assertEqual(result["name"], "John Doe")
        self.assertEqual(result["email"], "john@example.com")
        self.mock_db_client.get_user.assert_called_once_with(123)

    def test_get_user_details_not_found(self):
        self.mock_db_client.get_user.return_value = None
        result, status_code = self.user_service.get_user_details(999)
        self.assertEqual(status_code, 404)
        self.assertEqual(result["error"], "User not found")
        self.mock_db_client.get_user.assert_called_once_with(999)

    def test_create_user_success(self):
        test_user = {
            "id": 456,
            "name": "Alice Smith",
            "email": "alice@example.com"
        }
        self.mock_db_client.save_user.return_value = None
        result, status_code = self.user_service.create_user(test_user)
        self.assertEqual(status_code, 201)
        self.assertEqual(result["message"], "User created successfully")
        self.assertEqual(result["id"], 456)
        self.mock_db_client.save_user.assert_called_once_with(test_user)

    def test_create_user_missing_fields(self):
        result, status_code = self.user_service.create_user({
            "id": 789,
            "email": "bob@example.com"
        })
        self.assertEqual(status_code, 400)
        self.assertEqual(result["error"], "Name and email are required")
        result, status_code = self.user_service.create_user({
            "id": 789,
            "name": "Bob Johnson"
        })
        self.assertEqual(status_code, 400)
        self.assertEqual(result["error"], "Name and email are required")
        self.mock_db_client.save_user.assert_not_called()

    def test_create_user_database_error(self):
        test_user = {
            "id": 789,
            "name": "Error Case",
            "email": "error@example.com"
        }
        self.mock_db_client.save_user.side_effect = Exception("Database error")
        result, status_code = self.user_service.create_user(test_user)
        self.assertEqual(status_code, 500)
        self.assertEqual(result["error"], "Database error")
        self.mock_db_client.save_user.assert_called_once_with(test_user)


if __name__ == '__main__':
    unittest.main()
