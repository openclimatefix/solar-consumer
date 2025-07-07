# neso_solar_consumer/__init__.py
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("solar_consumer")
except PackageNotFoundError:
    __version__ = "v?"
