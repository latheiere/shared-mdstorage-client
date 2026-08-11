from importlib.metadata import PackageNotFoundError, version


try:
    __version__ = version("shared-mdstorage-client")
except PackageNotFoundError:
    __version__ = "0+uninstalled"
