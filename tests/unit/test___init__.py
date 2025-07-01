import importlib
from importlib.metadata import PackageNotFoundError
import sys
from unittest import TestCase, main, mock

import stpstone


class TestInit(TestCase):
    """Test cases for stpstone/__init__.py"""

    def test_version_exists(self):
        """Test that __version__ exists and is a string."""
        self.assertTrue(hasattr(stpstone, "__version__"))
        self.assertIsInstance(stpstone.__version__, str)
        self.assertTrue(len(stpstone.__version__) > 0)

    def test_path_extension(self):
        """Test that __path__ is properly extended."""
        self.assertTrue(hasattr(stpstone, "__path__"))
        self.assertIsInstance(stpstone.__path__, list)
        self.assertTrue(len(stpstone.__path__) > 0)

    @mock.patch("importlib.metadata.version", side_effect=PackageNotFoundError)
    @mock.patch("importlib.metadata.metadata", return_value={"version": "2.0.30"})
    def test_version_fallback_metadata(self, mock_metadata, mock_version):
        """Test version fallback to metadata when package not found."""
        # Reload the module to apply the mocks
        importlib.reload(sys.modules["stpstone"])
        self.assertEqual(stpstone.__version__, "2.0.30")
        mock_version.assert_called_once_with("stpstone")
        mock_metadata.assert_called_once_with("stpstone")

    @mock.patch("importlib.metadata.version", side_effect=PackageNotFoundError)
    @mock.patch("importlib.metadata.metadata", side_effect=PackageNotFoundError)
    def test_version_fallback_hardcoded(self, mock_metadata, mock_version):
        """Test version falls back to hardcoded value when all else fails."""
        # Reload the module to apply the mocks
        importlib.reload(sys.modules["stpstone"])
        self.assertEqual(stpstone.__version__, "2.0.30")
        mock_version.assert_called_once_with("stpstone")
        mock_metadata.assert_called_once_with("stpstone")

    @mock.patch("importlib.metadata.version", side_effect=PackageNotFoundError)
    @mock.patch("importlib.metadata.metadata", side_effect=ImportError)
    def test_version_fallback_import_error(self, mock_metadata, mock_version):
        """Test version falls back when importlib.metadata is not available."""
        # Reload the module to apply the mocks
        importlib.reload(sys.modules["stpstone"])
        self.assertEqual(stpstone.__version__, "2.0.30")
        mock_version.assert_called_once_with("stpstone")
        mock_metadata.assert_called_once_with("stpstone")

    def tearDown(self):
        """Ensure the original module is restored after each test."""
        importlib.reload(sys.modules["stpstone"])

if __name__ == "__main__":
    main()
