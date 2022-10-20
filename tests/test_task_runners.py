import asyncio
import logging
import sys
from functools import partial
from uuid import uuid4

import cloudpickle
import distributed
import pytest
from prefect import flow, task
from prefect.states import State
from prefect.task_runners import TaskConcurrencyType
from prefect.testing.fixtures import hosted_orion_api, use_hosted_orion  # noqa: F401
from prefect.testing.standard_test_suites import TaskRunnerStandardTestSuite
from prefect.utilities.annotations import allow_failure

from prefect_dask import DaskTaskRunner


@pytest.fixture(scope="session")
def event_loop(request):
    """
    Redefine the event loop to support session/module-scoped fixtures;
    see https://github.com/pytest-dev/pytest-asyncio/issues/68
    When running on Windows we need to use a non-default loop for subprocess support.
    """
    if sys.platform == "win32" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    policy = asyncio.get_event_loop_policy()

    if sys.version_info < (3, 8) and sys.platform != "win32":
        from prefect.utilities.compat import ThreadedChildWatcher

        # Python < 3.8 does not use a `ThreadedChildWatcher` by default which can
        # lead to errors in tests as the previous default `SafeChildWatcher`  is not
        # compatible with threaded event loops.
        policy.set_child_watcher(ThreadedChildWatcher())

    loop = policy.new_event_loop()

    # configure asyncio logging to capture long running tasks
    asyncio_logger = logging.getLogger("asyncio")
    asyncio_logger.setLevel("WARNING")
    asyncio_logger.addHandler(logging.StreamHandler())
    loop.set_debug(True)
    loop.slow_callback_duration = 0.25

    try:
        yield loop
    finally:
        loop.close()

    # Workaround for failures in pytest_asyncio 0.17;
    # see https://github.com/pytest-dev/pytest-asyncio/issues/257
    policy.set_event_loop(loop)


@pytest.fixture
def dask_task_runner_with_existing_cluster(use_hosted_orion):  # noqa
    """
    Generate a dask task runner that's connected to a local cluster
    """
    with distributed.LocalCluster(n_workers=2) as cluster:
        with distributed.Client(cluster) as client:
            address = client.scheduler.address
            yield DaskTaskRunner(address=address)


@pytest.fixture
def dask_task_runner_with_process_pool():
    yield DaskTaskRunner(cluster_kwargs={"processes": True})


@pytest.fixture
def dask_task_runner_with_thread_pool():
    yield DaskTaskRunner(cluster_kwargs={"processes": False})


@pytest.fixture
def default_dask_task_runner():
    yield DaskTaskRunner()


class TestDaskTaskRunner(TaskRunnerStandardTestSuite):
    @pytest.fixture(
        params=[
            default_dask_task_runner,
            dask_task_runner_with_existing_cluster,
            dask_task_runner_with_process_pool,
            dask_task_runner_with_thread_pool,
        ]
    )
    def task_runner(self, request):
        yield request.getfixturevalue(
            request.param._pytestfixturefunction.name or request.param.__name__
        )

    async def test_is_pickleable_after_start(self, task_runner):
        """
        The task_runner must be picklable as it is attached to `PrefectFuture` objects
        Reimplemented to set Dask client as default to allow unpickling
        """
        task_runner.client_kwargs["set_as_default"] = True
        async with task_runner.start():
            pickled = cloudpickle.dumps(task_runner)
            unpickled = cloudpickle.loads(pickled)
            assert isinstance(unpickled, type(task_runner))

    @pytest.mark.parametrize("exception", [KeyboardInterrupt(), ValueError("test")])
    async def test_wait_captures_exceptions_as_crashed_state(
        self, task_runner, exception
    ):
        """
        Dask wraps the exception, interrupts will result in "Cancelled" tasks
        or "Killed" workers while normal errors will result in the raw error with Dask.
        We care more about the crash detection and
        lack of re-raise here than the equality of the exception.
        """
        if task_runner.concurrency_type != TaskConcurrencyType.PARALLEL:
            pytest.skip(
                f"This will abort the run for "
                f"{task_runner.concurrency_type} task runners."
            )

        async def fake_orchestrate_task_run():
            raise exception

        test_key = uuid4()

        async with task_runner.start():
            await task_runner.submit(
                call=partial(fake_orchestrate_task_run),
                key=test_key,
            )

            state = await (await task_runner.wait(test_key, 5))
            assert state is not None, "wait timed out"
            assert isinstance(state, State), "wait should return a state"
            assert state.name == "Crashed"

    def test_dask_task_key_has_prefect_task_name(self):
        task_runner = DaskTaskRunner()

        @task
        def my_task():
            return 1

        @flow(task_runner=task_runner)
        def my_flow():
            my_task.submit()
            my_task.submit()
            my_task.submit()

        my_flow()
        futures = task_runner._dask_futures.values()
        # ensure task run name is in key
        assert all(future.key.startswith("my_task-") for future in futures)
        # ensure flow run retries is in key
        assert all(future.key.endswith("-1") for future in futures)


    @pytest.mark.parametrize("annotation_cls", [allow_failure])
    def test_works_with_futures_wrapped_in_annotations(self, annotation_cls):
        task_runner = DaskTaskRunner()

        @task
        def my_task(value):
            return value

        @flow(task_runner=task_runner)
        def my_flow():
            future = my_task.submit(1)
            return my_task.submit(annotation_cls(future))

        result = my_flow()
        assert result == annotation_cls(1)