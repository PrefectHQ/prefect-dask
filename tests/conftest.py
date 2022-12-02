import pytest
from prefect.settings import PREFECT_ASYNC_FETCH_STATE_RESULT, temporary_settings


@pytest.fixture(autouse=True)
def fetch_state_result():
    with temporary_settings(updates={PREFECT_ASYNC_FETCH_STATE_RESULT: True}):
        yield


# this crashes the tests "test_failing_flow_run";
# all tests pass without this fixture so will comment
# this out and defer for later investigation since
# users will not be using this in their code
# more info on the error below:
# https://github.com/PrefectHQ/prefect-dask/issues/55
# @pytest.fixture(scope="session", autouse=True)
# def prefect_db():
#     """
#     Sets up test harness for temporary DB during test runs.
#     """
#     with prefect_test_harness():
#         yield


@pytest.fixture(autouse=True)
def reset_object_registry():
    """
    Ensures each test has a clean object registry.
    """
    from prefect.context import PrefectObjectRegistry

    with PrefectObjectRegistry():
        yield
