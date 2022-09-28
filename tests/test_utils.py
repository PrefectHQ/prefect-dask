import dask
import pytest
from distributed import Client
from prefect import flow, task

from prefect_dask import DaskTaskRunner, get_dask_client


@pytest.mark.parametrize("timeout", [None, 10])
def test_get_dask_client_task_run_context_integration(timeout):
    @task
    def test_task():
        delayed_num = dask.delayed(42)
        with get_dask_client(timeout=timeout) as client:
            assert isinstance(client, Client)
            if timeout is not None:
                assert client._timeout == timeout
            result = client.compute(delayed_num).result()
        return result

    @flow(task_runner=DaskTaskRunner)
    def test_flow():
        future = test_task.submit()
        return future.result()

    assert test_flow() == 42


def test_get_dask_client_flow_run_context_integration():
    @flow(task_runner=DaskTaskRunner)
    def test_flow():
        delayed_num = dask.delayed(42)
        with get_dask_client() as client:
            assert isinstance(client, Client)
            result = client.compute(delayed_num, sync=True)
        return result

    assert test_flow() == 42


def test_get_dask_client_flow_run_context_catch_timeout_error():
    @flow(task_runner=DaskTaskRunner)
    def test_flow():
        with get_dask_client(timeout=42):
            pass

    with pytest.raises(ValueError, match="Passing `timeout`"):
        test_flow()


@pytest.mark.parametrize("timeout", [None, 10])
def test_get_dask_client_no_context(timeout):
    delayed_num = dask.delayed(42)
    with get_dask_client(timeout=timeout) as client:
        assert isinstance(client, Client)
        if timeout is not None:
            assert client._timeout == timeout
        result = client.compute(delayed_num).result()
    assert result == 42
