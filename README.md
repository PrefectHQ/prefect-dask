# prefect-dask

## Welcome!

Prefect integrations with the Dask execution framework.

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-dask` with `pip`:

```bash
pip install prefect-dask
```

### Write and run a flow

```python
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner

@task
def say_hello(name):
    print(f"hello {name}")

@task
def say_goodbye(name):
    print(f"goodbye {name}")

@flow(task_runner=DaskTaskRunner())
def greetings(names):
    for name in names:
        say_hello(name)
        say_goodbye(name)

if __name__ == "__main__":
    greetings(["arthur", "trillian", "ford", "marvin"])
```

## Resources

If you encounter any bugs while using `prefect-dask`, feel free to open an issue in the [prefect-dask](https://github.com/PrefectHQ/prefect-dask) repository.

If you have any questions or issues while using `prefect-dask`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-dask` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-dask.git

cd prefect-dask/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
