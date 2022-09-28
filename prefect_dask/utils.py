"""
Utils to use alongside prefect-dask.
"""

from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Dict, Optional, Union

from distributed import Client, get_client
from prefect.context import FlowRunContext, TaskRunContext


@contextmanager
def get_dask_client(
    timeout: Optional[Union[int, float, str, timedelta]] = None,
    **client_kwargs: Dict[str, Any]
) -> Client:
    """
    Yields a temporary client; this is useful for parallelizing operations
    on dask collections, such as a `dask.DataFrame` or `dask.Bag`.

    Without invoking this, workers do not automatically get a client to connect
    to the full cluster. Therefore, it will attempt perform work within the
    worker itself serially, and potentially overwhelming the single worker.

    Args:
        timeout: Timeout after which to error out; has no effect in
            flow run contexts because the client has already started;
            Defaults to the `distributed.comm.timeouts.connect`
            configuration value.
        client_kwargs: Additional keyword arguments to pass to
            `distributed.Client`, and overwrites inherited keyword arguments
            from the task runner, if any.

    Returns:
        Within task run contexts, the dask worker client, and within flow run contexts,
        the existing client used in `DaskTaskRunner`.

    Examples:
        Use `get_dask_client` to distribute work across workers within task run context.
        ```python
        import dask
        from prefect import flow, task
        from prefect_dask import DaskTaskRunner, get_dask_client

        @task
        def compute_task():
            with get_dask_client(timeout="120s") as client:
                df = dask.datasets.timeseries("2000", "2001", partition_freq="4w")
                summary_df = df.describe().compute()
            return summary_df

        @flow(task_runner=DaskTaskRunner())
        def dask_flow():
            prefect_future = compute_task.submit()
            return prefect_future.result()

        dask_flow()
        ```
    """
    flow_run_context = FlowRunContext.get()
    task_run_context = TaskRunContext.get()

    if flow_run_context:
        task_runner = flow_run_context.task_runner
        input_client_kwargs = task_runner.client_kwargs
        address = task_runner._connect_to
        asynchronous = flow_run_context.flow.isasync
    elif task_run_context:
        # copies functionality of worker_client(separate_thread=False)
        # because this allows us to set asynchronous based on user's task
        input_client_kwargs = {}
        address = get_client(timeout=timeout).scheduler.address
        asynchronous = task_run_context.task.isasync
    else:
        # this else clause allows users to debug or test
        # without much change to code
        input_client_kwargs = {}
        address = None
        asynchronous = False

    input_client_kwargs["address"] = address
    input_client_kwargs["asynchronous"] = asynchronous
    if timeout is not None:
        input_client_kwargs["timeout"] = timeout
    input_client_kwargs.update(**client_kwargs)

    with Client(**input_client_kwargs) as client:
        yield client
