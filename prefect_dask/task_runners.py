"""
Interface and implementations of the Dask Task Runner.
[Task Runners](https://orion-docs.prefect.io/api-ref/prefect/task-runners/)
in Prefect are responsible for managing the execution of Prefect task runs.
Generally speaking, users are not expected to interact with
task runners outside of configuring and initializing them for a flow.

Example:
    ```python
    from prefect import flow, task
    from prefect.task_runners import SequentialTaskRunner
    from typing import List

    @task
    def say_hello(name):
        print(f"hello {name}")

    @task
    def say_goodbye(name):
        print(f"goodbye {name}")

    @flow(task_runner=SequentialTaskRunner())
    def greetings(names: List[str]):
        for name in names:
            say_hello(name)
            say_goodbye(name)
    ```

    Switching to a `DaskTaskRunner`:
    ```python
    from prefect_dask.task_runners import DaskTaskRunner
    flow.task_runner = DaskTaskRunner()
    greetings(["arthur", "trillian", "ford", "marvin"])
    ```

    Output:
    ```
    hello arthur
    goodbye arthur
    hello trillian
    hello ford
    goodbye marvin
    hello marvin
    goodbye ford
    goodbye trillian
    ```
"""

from contextlib import AsyncExitStack
from typing import Any, Awaitable, Callable, Dict, Optional, Union

import distributed
from prefect.futures import PrefectFuture
from prefect.orion.schemas.core import TaskRun
from prefect.orion.schemas.states import State
from prefect.states import exception_to_crashed_state
from prefect.task_runners import BaseTaskRunner, R, TaskConcurrencyType
from prefect.utilities.asyncutils import A
from prefect.utilities.collections import visit_collection
from prefect.utilities.importtools import from_qualified_name, to_qualified_name


class DaskTaskRunner(BaseTaskRunner):
    """
    A parallel task_runner that submits tasks to the `dask.distributed` scheduler.
    By default a temporary `distributed.LocalCluster` is created (and
    subsequently torn down) within the `start()` contextmanager. To use a
    different cluster class (e.g.
    [`dask_kubernetes.KubeCluster`](https://kubernetes.dask.org/)), you can
    specify `cluster_class`/`cluster_kwargs`.
    Alternatively, if you already have a dask cluster running, you can provide
    the address of the scheduler via the `address` kwarg.
    !!! warning "Multiprocessing safety"
        Note that, because the `DaskTaskRunner` uses multiprocessing, calls to flows
        in scripts must be guarded with `if __name__ == "__main__":` or warnings will
        be displayed.
    Args:
        address (string, optional): Address of a currently running dask
            scheduler; if one is not provided, a temporary cluster will be
            created in `DaskTaskRunner.start()`.  Defaults to `None`.
        cluster_class (string or callable, optional): The cluster class to use
            when creating a temporary dask cluster. Can be either the full
            class name (e.g. `"distributed.LocalCluster"`), or the class itself.
        cluster_kwargs (dict, optional): Additional kwargs to pass to the
            `cluster_class` when creating a temporary dask cluster.
        adapt_kwargs (dict, optional): Additional kwargs to pass to `cluster.adapt`
            when creating a temporary dask cluster. Note that adaptive scaling
            is only enabled if `adapt_kwargs` are provided.
        client_kwargs (dict, optional): Additional kwargs to use when creating a
            [`dask.distributed.Client`](https://distributed.dask.org/en/latest/api.html#client).
    Examples:
        Using a temporary local dask cluster:
        >>> from prefect import flow
        >>> from prefect_dask.task_runners import DaskTaskRunner
        >>> @flow(task_runner=DaskTaskRunner)
        >>> def my_flow():
        >>>     ...
        Using a temporary cluster running elsewhere. Any Dask cluster class should
        work, here we use [dask-cloudprovider](https://cloudprovider.dask.org):
        >>> DaskTaskRunner(
        >>>     cluster_class="dask_cloudprovider.FargateCluster",
        >>>     cluster_kwargs={
        >>>          "image": "prefecthq/prefect:latest",
        >>>          "n_workers": 5,
        >>>     },
        >>> )
        Connecting to an existing dask cluster:
        >>> DaskTaskRunner(address="192.0.2.255:8786")
    """

    def __init__(
        self,
        address: str = None,
        cluster_class: Union[str, Callable] = None,
        cluster_kwargs: dict = None,
        adapt_kwargs: dict = None,
        client_kwargs: dict = None,
    ):
        # Validate settings and infer defaults
        if address:
            if cluster_class or cluster_kwargs or adapt_kwargs:
                raise ValueError(
                    "Cannot specify `address` and "
                    "`cluster_class`/`cluster_kwargs`/`adapt_kwargs`"
                )
        else:
            if isinstance(cluster_class, str):
                cluster_class = from_qualified_name(cluster_class)
            else:
                cluster_class = cluster_class

        # Create a copies of incoming kwargs since we may mutate them
        cluster_kwargs = cluster_kwargs.copy() if cluster_kwargs else {}
        adapt_kwargs = adapt_kwargs.copy() if adapt_kwargs else {}
        client_kwargs = client_kwargs.copy() if client_kwargs else {}

        # Update kwargs defaults
        client_kwargs.setdefault("set_as_default", False)

        # The user cannot specify async/sync themselves
        if "asynchronous" in client_kwargs:
            raise ValueError(
                "`client_kwargs` cannot set `asynchronous`. "
                "This option is managed by Prefect."
            )
        if "asynchronous" in cluster_kwargs:
            raise ValueError(
                "`cluster_kwargs` cannot set `asynchronous`. "
                "This option is managed by Prefect."
            )

        # Store settings
        self.address = address
        self.cluster_class = cluster_class
        self.cluster_kwargs = cluster_kwargs
        self.adapt_kwargs = adapt_kwargs
        self.client_kwargs = client_kwargs

        # Runtime attributes
        self._client: "distributed.Client" = None
        self._cluster: "distributed.deploy.Cluster" = None
        self._dask_futures: Dict[str, "distributed.Future"] = {}

        super().__init__()

    @property
    def concurrency_type(self) -> TaskConcurrencyType:
        return (
            TaskConcurrencyType.PARALLEL
            if self.cluster_kwargs.get("processes")
            else TaskConcurrencyType.CONCURRENT
        )

    async def submit(
        self,
        task_run: TaskRun,
        run_key: str,
        run_fn: Callable[..., Awaitable[State[R]]],
        run_kwargs: Dict[str, Any],
        asynchronous: A = True,
    ) -> PrefectFuture[R, A]:
        if not self._started:
            raise RuntimeError(
                "The task runner must be started before submitting work."
            )

        # Cast Prefect futures to Dask futures where possible to optimize Dask task
        # scheduling
        run_kwargs = await self._optimize_futures(run_kwargs)

        self._dask_futures[run_key] = self._client.submit(
            run_fn,
            # Dask displays the text up to the first '-' as the name, the task run key
            # should include the task run name for readability in the dask console.
            key=run_key,
            # Dask defaults to treating functions are pure, but we set this here for
            # explicit expectations. If this task run is submitted to Dask twice, the
            # result of the first run should be returned. Subsequent runs would return
            # `Abort` exceptions if they were submitted again.
            pure=True,
            **run_kwargs,
        )

        return PrefectFuture(
            task_run=task_run,
            task_runner=self,
            run_key=run_key,
            asynchronous=asynchronous,
        )

    def _get_dask_future(self, prefect_future: PrefectFuture) -> "distributed.Future":
        """
        Retrieve the dask future corresponding to a Prefect future.
        The Dask future is for the `run_fn`, which should return a `State`.
        """
        return self._dask_futures[prefect_future.run_key]

    async def _optimize_futures(self, expr):
        async def visit_fn(expr):
            if isinstance(expr, PrefectFuture):
                dask_future = self._dask_futures.get(expr.run_key)
                if dask_future is not None:
                    return dask_future
            # Fallback to return the expression unaltered
            return expr

        return await visit_collection(expr, visit_fn=visit_fn, return_data=True)

    async def wait(
        self,
        prefect_future: PrefectFuture,
        timeout: float = None,
    ) -> Optional[State]:
        future = self._get_dask_future(prefect_future)
        try:
            return await future.result(timeout=timeout)
        except distributed.TimeoutError:
            return None
        except BaseException as exc:
            return exception_to_crashed_state(exc)

    async def _start(self, exit_stack: AsyncExitStack):
        """
        Start the task runner and prep for context exit.
        - Creates a cluster if an external address is not set.
        - Creates a client to connect to the cluster.
        - Pushes a call to wait for all running futures to complete on exit.
        """
        if self.address:
            self.logger.info(
                f"Connecting to an existing Dask cluster at {self.address}"
            )
            connect_to = self.address
        else:
            self.cluster_class = self.cluster_class or distributed.LocalCluster

            self.logger.info(
                f"Creating a new Dask cluster with "
                f"`{to_qualified_name(self.cluster_class)}`"
            )
            connect_to = self._cluster = await exit_stack.enter_async_context(
                self.cluster_class(asynchronous=True, **self.cluster_kwargs)
            )
            if self.adapt_kwargs:
                self._cluster.adapt(**self.adapt_kwargs)

        self._client = await exit_stack.enter_async_context(
            distributed.Client(connect_to, asynchronous=True, **self.client_kwargs)
        )

        if self._client.dashboard_link:
            self.logger.info(
                f"The Dask dashboard is available at {self._client.dashboard_link}",
            )

    def __getstate__(self):
        """
        Allow the `DaskTaskRunner` to be serialized by dropping
        the `distributed.Client`, which contains locks.
        Must be deserialized on a dask worker.
        """
        data = self.__dict__.copy()
        data.update({k: None for k in {"_client", "_cluster"}})
        return data

    def __setstate__(self, data: dict):
        """
        Restore the `distributed.Client` by loading the client on a dask worker.
        """
        self.__dict__.update(data)
        self._client = distributed.get_client()
