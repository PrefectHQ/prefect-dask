import dask
import pytest
from distributed import Client
from prefect import flow, task

from prefect_dask import DaskTaskRunner, get_dask_async_client, get_dask_sync_client
from prefect_dask.exceptions import ImproperClientError


class TestDaskSyncClient:
    def test_from_task(self):
        @task
        def test_task():
            delayed_num = dask.delayed(42)
            with get_dask_sync_client() as client:
                assert isinstance(client, Client)
                result = client.compute(delayed_num).result()
            return result

        @flow(task_runner=DaskTaskRunner)
        def test_flow():
            future = test_task.submit()
            return future.result()

        assert test_flow() == 42

    def test_from_async_task_error(self):
        @task
        async def test_task():
            with get_dask_sync_client():
                pass

        @flow(task_runner=DaskTaskRunner)
        def test_flow():
            test_task.submit()

        match = "The task run is async"
        with pytest.raises(ImproperClientError, match=match):
            test_flow()

    def test_from_flow(self):
        @flow(task_runner=DaskTaskRunner)
        def test_flow():
            delayed_num = dask.delayed(42)
            with get_dask_sync_client() as client:
                assert isinstance(client, Client)
                result = client.compute(delayed_num).result()
            return result

        assert test_flow() == 42

    async def test_from_async_flow_error(self):
        @flow(task_runner=DaskTaskRunner)
        async def test_flow():
            with get_dask_sync_client():
                pass

        match = "The flow run is async"
        with pytest.raises(ImproperClientError, match=match):
            await test_flow()

    def test_outside_run_context(self):
        delayed_num = dask.delayed(42)
        with get_dask_sync_client() as client:
            assert isinstance(client, Client)
            result = client.compute(delayed_num).result()
        assert result == 42

    @pytest.mark.parametrize("timeout", [None, 8])
    def test_include_timeout(self, timeout):
        delayed_num = dask.delayed(42)
        with get_dask_sync_client(timeout=timeout) as client:
            assert isinstance(client, Client)
            if timeout is not None:
                assert client._timeout == timeout
            result = client.compute(delayed_num).result()
        assert result == 42


class TestDaskAsyncClient:
    async def test_from_task(self):
        @task
        async def test_task():
            delayed_num = dask.delayed(42)
            async with get_dask_async_client() as client:
                assert isinstance(client, Client)
                result = await client.compute(delayed_num).result()
            return result

        @flow(task_runner=DaskTaskRunner)
        async def test_flow():
            future = await test_task.submit()
            return await future.result()

        assert (await test_flow()) == 42

    async def test_from_async_task_error(self):
        @task
        async def test_task():
            with get_dask_sync_client():
                pass

        @flow(task_runner=DaskTaskRunner)
        async def test_flow():
            await test_task.submit()

        match = "The task run is sync"
        with pytest.raises(ImproperClientError, match=match):
            await test_flow()

    async def test_from_flow(self):
        @flow(task_runner=DaskTaskRunner)
        async def test_flow():
            delayed_num = dask.delayed(42)
            async with get_dask_async_client() as client:
                assert isinstance(client, Client)
                result = await client.compute(delayed_num).result()
            return result

        assert (await test_flow()) == 42

    async def test_from_async_flow_error(self):
        @flow(task_runner=DaskTaskRunner)
        async def test_flow():
            with get_dask_sync_client():
                pass

        match = "The flow run is async"
        with pytest.raises(ImproperClientError, match=match):
            await test_flow()

    async def test_outside_run_context(self):
        delayed_num = dask.delayed(42)
        async with get_dask_async_client() as client:
            assert isinstance(client, Client)
            result = await client.compute(delayed_num).result()
        assert result == 42

    @pytest.mark.parametrize("timeout", [None, 8])
    async def test_include_timeout(self, timeout):
        delayed_num = dask.delayed(42)
        async with get_dask_async_client(timeout=timeout) as client:
            assert isinstance(client, Client)
            if timeout is not None:
                assert client._timeout == timeout
            result = await client.compute(delayed_num).result()
        assert result == 42
