from datetime import timedelta

import dask
import pytest
from distributed import Client
from prefect import flow, task

from prefect_dask import DaskTaskRunner, get_dask_client


@pytest.mark.parametrize("timeout", [10, 10.0, "10s", timedelta(seconds=10)])
def test_get_dask_client_integration(timeout):
    @task
    def test_task():
        delayed_num = dask.delayed(42)
        with get_dask_client(timeout=timeout) as client:
            assert isinstance(client, Client)
            future = client.compute(delayed_num)
        return future.result()

    @flow(task_runner=DaskTaskRunner)
    def test_flow():
        future = test_task.submit()
        return future.result()

    assert test_flow() == 42
