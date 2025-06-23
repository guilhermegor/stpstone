from importlib.metadata import version

__version__ = version(__name__)

__path__ = __import__("pkgutil").extend_path(__path__, __name__)
