# Coordinate, and parallelize, your dataflow

The `prefect-dask` collection makes it easy to parallelize your flows!

## Example Scenario

Suppose you're working on a deep learning project and need to download many images to ingest into your model.

This can be a slow process if done sequentially, but `prefect-dask` can help by parallelizing the tasks and completing the work much faster.

## Integrating with Existing Flows

Integrating `prefect-dask` into your existing flow is as easy as 1-2-3!

1. Include the import: `from prefect_dask import DaskTaskRunner`
2. Specify the task runner in the flow decorator: `@flow(task_runner=DaskTaskRunner)`
3. Submit existing tasks to the task runners: `a_task.submit(*args, **kwargs)`

=== "Before"

    ```python
    # Completed in 15.2 seconds

    from typing import List
    from pathlib import Path

    import httpx
    from prefect import flow, task


    URL_FORMAT = (
        "https://www.cpc.ncep.noaa.gov/products/NMME/archive/"
        "{year:04d}{month:02d}0800/current/images/nino34.rescaling.ENSMEAN.png"
    )

    @task
    def download_image(year: int, month: int, directory: Path) -> Path:
        # download image from URL
        url = URL_FORMAT.format(year=year, month=month)
        resp = httpx.get(url)

        # save content to directory/YYYYMM.png
        file_path = (directory / url.split("/")[-1]).with_stem(f"{year:04d}{month:02d}")
        file_path.write_bytes(resp.content)
        return file_path

    @flow
    def download_nino_34_plumes_from_year(year: int) -> List[Path]:
        # create a directory to hold images
        directory = Path("data")
        directory.mkdir(exist_ok=True)

        # download all images
        file_paths = []
        for month in range(1, 12 + 1):
            file_path = download_image(year, month, directory)
            file_paths.append(file_path)
        return file_paths

    if __name__ == "__main__":
        download_nino_34_plumes_from_year(2022)
    ```

=== "After"

    ```python
    # Completed in 5.7 seconds

    from typing import List
    from pathlib import Path

    import httpx
    from prefect import flow, task
    from prefect_dask import DaskTaskRunner

    URL_FORMAT = (
        "https://www.cpc.ncep.noaa.gov/products/NMME/archive/"
        "{year:04d}{month:02d}0800/current/images/nino34.rescaling.ENSMEAN.png"
    )

    @task
    def download_image(year: int, month: int, directory: Path) -> Path:
        # download image from URL
        url = URL_FORMAT.format(year=year, month=month)
        resp = httpx.get(url)

        # save content to directory/YYYYMM.png
        file_path = (directory / url.split("/")[-1]).with_stem(f"{year:04d}{month:02d}")
        file_path.write_bytes(resp.content)
        return file_path

    @flow(task_runner=DaskTaskRunner(cluster_kwargs={"processes": False}))
    def download_nino_34_plumes_from_year(year: int) -> List[Path]:
        # create a directory to hold images
        directory = Path("data")
        directory.mkdir(exist_ok=True)

        # download all images
        file_paths = []
        for month in range(1, 12 + 1):
            file_path = download_image.submit(year, month, directory)
            file_paths.append(file_path)
        return file_paths

    if __name__ == "__main__":
        download_nino_34_plumes_from_year(2022)
    ```

The original flow completes in **15.2 seconds**--not bad.

However, with just a few minor tweaks, we were able to reduce the runtime by nearly three folds, down to just **5.7** seconds!

## Working with Dask Collections

If you have Dask Collections, like a `dask.DataFrame`, and would like to add observability to your work, `prefect-dask` can do that too!

-- TODO: insert more here --

```python
import dask
from prefect import flow, task
from prefect_dask import DaskTaskRunner, get_dask_client

@task
def compute_task():
    with get_dask_client() as client:
        df = dask.datasets.timeseries("2000", "2001", partition_freq="4w")
        summary_df = df.describe().compute()
    return summary_df

@flow(task_runner=DaskTaskRunner())
def dask_flow():
    prefect_future = compute_task.submit()
    return prefect_future.result()

dask_flow()
```

## Conclusion

It's easy to use Dask to run tasks in parallel and fun to see runtimes reduce significantly!

Get started now by [installing `prefect-dask`](/#python-setup).
