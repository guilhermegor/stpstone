import unittest
from unittest.mock import MagicMock, patch, call
import pandas as pd

from stpstone.utils.connections.databases.postgresql import PostgreSQLDB


class TestPostgreSQLDB(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.mock_connect_patch = patch(
            "stpstone.utils.connections.databases.postgresql.connect",
            return_value=self.mock_conn,
        )
        self.mock_connect = self.mock_connect_patch.start()

        self.db = PostgreSQLDB(
            dbname="testdb",
            user="user",
            password="pass",
            host="localhost",
            port=5432,
        )

    def tearDown(self):
        self.mock_connect_patch.stop()

    def test_execute(self):
        self.db.execute("SELECT 1")
        self.mock_cursor.execute.assert_called_with("SELECT 1")

    def test_read_returns_dataframe(self):
        self.mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        df = self.db.read("SELECT * FROM users")
        self.mock_cursor.execute.assert_called_with("SELECT * FROM users")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertListEqual(df["name"].tolist(), ["Alice", "Bob"])

    @patch("stpstone.utils.connections.databases.postgresql.JsonFiles")
    def test_insert_success(self, mock_jsonfiles):
        json_data = [{"id": 1, "name": "Test"}]
        mock_jsonfiles.return_value.normalize_json_keys.return_value = json_data

        self.db.insert(json_data, "users")
        self.mock_cursor.executemany.assert_called()
        self.mock_conn.commit.assert_called()

    @patch("stpstone.utils.connections.databases.postgresql.JsonFiles")
    def test_insert_failure_rolls_back(self, mock_jsonfiles):
        json_data = [{"id": 1, "name": "Test"}]
        mock_jsonfiles.return_value.normalize_json_keys.return_value = json_data

        self.mock_cursor.executemany.side_effect = Exception("Insert failed")

        with self.assertRaises(Exception) as cm:
            self.db.insert(json_data, "users")

        self.mock_conn.rollback.assert_called()
        self.mock_conn.close.assert_called()
        self.assertIn("Insert failed", str(cm.exception))

    @patch("os.makedirs")
    @patch("subprocess.run")
    def test_backup_success(self, mock_run, mock_makedirs):
        result = self.db.backup("/tmp/backup", "dump.bak")
        self.assertIn("Backup successful", result)
        mock_run.assert_called()

    @patch("subprocess.run", side_effect=Exception("Failed"))
    def test_backup_failure(self, mock_run):
        result = self.db.backup("/tmp/backup", "dump.bak")
        self.assertIn("An error occurred", result)

    def test_close_connection(self):
        self.db.close()
        self.mock_conn.close.assert_called()


if __name__ == "__main__":
    unittest.main()
