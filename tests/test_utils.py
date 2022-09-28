import dask
import pytest
from distributed import Client
from prefect import flow, task

from prefect_dask import DaskTaskRunner, get_dask_client


@pytest.mark.parametrize("timeout", [None, 10])
def test_get_dask_client_from_task(timeout):
    @task
    def test_task():
        delayed_num = dask.delayed(42)
        with get_dask_client(timeout=timeout) as client:
            assert isinstance(client, Client)
            if timeout is not None:
                assert client._timeout == timeout
            result = delayed_num.compute()
        return result

    @flow(task_runner=DaskTaskRunner)
    def test_flow():
        future = test_task.submit()
        return future.result()

    assert test_flow() == 42


@pytest.mark.parametrize("timeout", [None, 10])
def test_get_dask_client_from_flow(timeout):
    @flow(task_runner=DaskTaskRunner)
    def test_flow():
        delayed_num = dask.delayed(42)
        with get_dask_client(timeout=timeout) as client:
            assert isinstance(client, Client)
            if timeout is not None:
                assert client._timeout == timeout
            result = delayed_num.compute()
        return result

    assert test_flow() == 42


@pytest.mark.parametrize("timeout", [None, 10])
def test_get_dask_client_outside_run_context(timeout):
    delayed_num = dask.delayed(42)
    with get_dask_client(timeout=timeout) as client:
        assert isinstance(client, Client)
        if timeout is not None:
            assert client._timeout == timeout
        result = delayed_num.compute()
    assert result == 42
