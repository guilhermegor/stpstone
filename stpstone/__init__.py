from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("stpstone")
except PackageNotFoundError:
    try:
        from importlib.metadata import metadata

        __version__ = metadata("stpstone")["version"]
    except (PackageNotFoundError, ImportError):
        __version__ = "2.0.28"

__path__ = __import__("pkgutil").extend_path(__path__, __name__)
