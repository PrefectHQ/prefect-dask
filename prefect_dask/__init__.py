from . import _version
from .task_runners import DaskTaskRunner  # noqa
from .utils import get_dask_client, get_dask_async_client  # noqa

__version__ = _version.get_versions()["version"]
