from . import _version
from .task_runners import DaskTaskRunner  # noqa

__version__ = _version.get_versions()["version"]
