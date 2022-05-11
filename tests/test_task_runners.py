from uuid import uuid4

import cloudpickle
import pytest
from prefect.orion.schemas.core import TaskRun
from prefect.states import State
from prefect.task_runners import TaskConcurrencyType
from prefect.testing.standard_test_suites import TaskRunnerStandardTestSuite

from .conftest import (
    dask_task_runner_with_existing_cluster,
    dask_task_runner_with_process_pool,
    dask_task_runner_with_thread_pool,
    default_dask_task_runner,
)


@pytest.mark.service("dask")
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

        task_run = TaskRun(flow_run_id=uuid4(), task_key="foo", dynamic_key="bar")

        async def fake_orchestrate_task_run():
            raise exception

        async with task_runner.start():
            future = await task_runner.submit(
                task_run=task_run, run_fn=fake_orchestrate_task_run, run_kwargs={}
            )

            state = await task_runner.wait(future, 5)
            assert state is not None, "wait timed out"
            assert isinstance(state, State), "wait should return a state"
            assert state.name == "Crashed"
