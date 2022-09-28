"""
Utils to use alongside prefect-dask.
"""

from contextlib import contextmanager
from datetime import timedelta
from typing import Optional, Union

from distributed import Client, worker_client
from prefect.context import FlowRunContext, TaskRunContext


@contextmanager
def get_dask_client(timeout: Optional[Union[int, float, str, timedelta]] = None):
    """
    This is useful for parallelizing operations on dask collections,
    such as a `dask.DataFrame`.

    Without invoking this, workers do not automatically get a client to connect
    to the full cluster. Therefore, it will attempt perform work within the
    worker itself serially, and potentially overwhelming the single worker.

    Within the task run context, this context manager is a simple
    wrapper around `distributed.worker_client` with `separate_thread=False` fixed
    Within the flow run context, this context manager simply returns
    the existing client used in `DaskTaskRunner`.

    Args:
        timeout: Timeout after which to error out; has no effect in
            flow run contexts because the client has already started;
            Defaults to the `distributed.comm.timeouts.connect`
            configuration value.

    Returns:
        Within task run contexts, the dask worker client, and within flow run contexts,
        the existing client used in `DaskTaskRunner`.

    Examples:
        Use `get_dask_client` to distribute work across workers within task run context.
        Be mindful of the futures upon `submit` and `compute`. To resolve these futures,
        call `result` on the future.
        ```python
        import dask
        from prefect import flow, task
        from prefect_dask import DaskTaskRunner, get_dask_client

        @task
        def compute_task():
            with get_dask_client(timeout="120s") as client:
                df = dask.datasets.timeseries("2000", "2001", partition_freq="4w")
                summary_df = client.compute(df.describe()).result()
            return summary_df

        @flow(task_runner=DaskTaskRunner())
        def dask_flow():
            prefect_future = compute_task.submit()
            return prefect_future.result()

        dask_flow()
        ```
    """
    task_run_context = TaskRunContext.get()
    flow_run_context = FlowRunContext.get()

    if task_run_context:
        with worker_client(timeout=timeout, separate_thread=False) as client:
            yield client
    elif flow_run_context:
        task_runner = flow_run_context.task_runner
        connect_to = task_runner._connect_to
        client_kwargs = task_runner.client_kwargs
        with Client(connect_to, **client_kwargs) as client:
            yield client
    else:
        # this else clause allows users to debug or test
        # without much change to code
        client_kwargs = {}
        if timeout is not None:  # dask errors if timeout=None here
            client_kwargs["timeout"] = timeout
        with Client(**client_kwargs) as client:
            yield client
