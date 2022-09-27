"""
Utils to use alongside prefect-dask.
"""

from contextlib import contextmanager
from datetime import timedelta
from typing import Optional, Union

from distributed import worker_client


@contextmanager
def get_dask_client(timeout: Optional[Union[int, float, str, timedelta]] = None):
    """
    This is intended to be called within tasks that run on workers, and is
    useful for operating on dask collections, such as a `dask.DataFrame`.

    Without invoking this, workers in a task do not automatically
    get a client to connect to the full cluster. Therefore, it will attempt
    perform work within the worker itself serially, and potentially overwhelming
    the single worker.

    Internally, this context manager is a simple wrapper around
    `distributed.worker_client` with `separate_thread=False` fixed.

    Args:
        timeout: Timeout after which to error out. Defaults to the
            `distributed.comm.timeouts.connect` configuration value.

    Returns:
        The dask worker client.

    Examples:
        Use `get_dask_client` to distribute work across workers.
        ```python
        import dask
        from prefect import flow, task
        from prefect_dask import DaskTaskRunner, get_dask_client

        @task
        def compute_task():
            with get_dask_client() as client:
                df = dask.datasets.timeseries("2000", "2005", partition_freq="2w")
                summary_df = df.describe()
                client.compute(summary_df)

        @flow(task_runner=DaskTaskRunner())
        def dask_flow():
            compute_task.submit()

        if __name__ == "__main__":
            dask_flow()
        ```
    """
    with worker_client(timeout=timeout, separate_thread=False) as client:
        yield client
