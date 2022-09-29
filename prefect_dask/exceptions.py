"""
Exceptions specific to prefect-dask.
"""


class ImproperClientError(Exception):
    """
    Raised when the flow/task is async but the client is sync, or vice versa.
    """
