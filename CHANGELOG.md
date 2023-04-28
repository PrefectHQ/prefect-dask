# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Updated `DaskTaskRunner` to accept `dask.deploy.Cluster` object for connecting to existing cluster - [#29](https://github.com/PrefectHQ/prefect-dask/pull/29)
- `get_dask_client` and `get_async_dask_client` allowing for distributed computation within a task - [#33](https://github.com/PrefectHQ/prefect-dask/pull/33)

### Changed

### Deprecated

### Removed

### Fixed

### Security

## 0.2.4

Released on April 27th, 2023.

## Added

- `DaskTaskRunner.duplicate` to support using the `DaskTaskRunner` in concurrent flow runs - [#93](https://github.com/PrefectHQ/prefect-dask/pull/93)

### Fixed

- Capped `distributed` version to `2023.3.1` due to issues with `get_dask_client` and `get_async_dask_client` with newer versions - [#93](https://github.com/PrefectHQ/prefect-dask/pull/93)
- Bug preventing using a retrieved async client and a local cluster - [#93](https://github.com/PrefectHQ/prefect-dask/pull/93)

## 0.2.3

Released on February 21st, 2023.

### Fixed

- Updated `prefect.orion` imports to `prefect.server` - [#82](https://github.com/PrefectHQ/prefect-dask/pull/82)
- Updated `DaskTaskRunner` to await the `adapt` method call of a dask cluster instance if it's an asynchronous method - [#77](https://github.com/PrefectHQ/prefect-dask/pull/77)

## 0.2.2

Released on December 1st, 2022.

- Updated `DaskTaskRunner` to handle task exceptions correctly in Prefect >= 2.6.0 - [#52](https://github.com/PrefectHQ/prefect-dask/pull/52)

## 0.2.1

Released on October 3rd, 2022.

### Added

- `get_dask_client` and `get_async_dask_client` allowing for distributed computation within a task - [#33](https://github.com/PrefectHQ/prefect-dask/pull/33)

### Fixed

- Make the task names that appear in the Dask dashboard match the Prefect task names - [#31](https://github.com/PrefectHQ/prefect-dask/pull/31)

## 0.2.0

Released on September 5th, 2022.

### Fixed

- Updated `DaskTaskRunner` to be compatible with the updated `TaskRunner` interface in the Prefect Core library (v2.3.0) - [#21](https://github.com/PrefectHQ/prefect-dask/pull/21)
- Fixed a bug where `optimize_futures` was awaiting `visit_collection`, leading to strange behavior - [#21](https://github.com/PrefectHQ/prefect-dask/pull/21)

## 0.1.2

Released on July 20th, 2022.

- Updated `DaskTaskRunner` to be compatible with core Prefect library (v2.0b9) - [#15](https://github.com/PrefectHQ/prefect-dask/pull/15)

## 0.1.1

Released on July 8th, 2022.

### Changed

- Updated `DaskTaskRunner` to be compatible with core Prefect library (v2.08b) - [#12](https://github.com/PrefectHQ/prefect-dask/pull/12)

## 0.1.0

Released on June 7th, 2022.

### Added

- Migrated `DaskTaskRunner` from core Prefect library - [#2](https://github.com/PrefectHQ/prefect-dask/pull/2)
- Expanded documentation [#9](https://github.com/PrefectHQ/prefect-dask/pull/9)
