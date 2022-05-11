import asyncio

import distributed
import pytest
from prefect.testing.fixtures import hosted_orion_api, use_hosted_orion  # noqa: F401

from prefect_dask import DaskTaskRunner


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture
@pytest.mark.service("dask")
def dask_task_runner_with_existing_cluster(use_hosted_orion):  # noqa
    """
    Generate a dask task runner that's connected to a local cluster
    """
    with distributed.LocalCluster(n_workers=2) as cluster:
        with distributed.Client(cluster) as client:
            address = client.scheduler.address
            yield DaskTaskRunner(address=address)


@pytest.fixture
@pytest.mark.service("dask")
def dask_task_runner_with_process_pool():
    yield DaskTaskRunner(cluster_kwargs={"processes": True})


@pytest.fixture
@pytest.mark.service("dask")
def dask_task_runner_with_thread_pool():
    yield DaskTaskRunner(cluster_kwargs={"processes": False})


@pytest.fixture
@pytest.mark.service("dask")
def default_dask_task_runner():
    yield DaskTaskRunner()
