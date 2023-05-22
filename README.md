
[![tests](https://github.com/ghga-de/ghga-connector/actions/workflows/unit_and_int_tests.yaml/badge.svg)](https://github.com/ghga-de/ghga-connector/actions/workflows/unit_and_int_tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/ghga-connector/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/ghga-connector?branch=main)

# Ghga Connector

GHGA Connector - A CLI client application for interacting with the GHGA system.

## Description

<!-- Please provide a short overview of the features of this service.-->


This package uses PycURL and thus has curl (https://curl.se/) as a dependency. On Debian-based Linux distributions, you can install cURL using:

```
sudo apt install libcurl4-openssl-dev libssl-dev
```


## Installation
We recommend using the provided Docker container.

A pre-build version is available at [docker hub](https://hub.docker.com/repository/docker/ghga/ghga-connector):
```bash
docker pull ghga/ghga-connector:0.2.13
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
docker build -t ghga/ghga-connector:0.2.13 .
```

For production-ready deployment, we recommend using Kubernetes, however,
for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is preconfigured:
docker run -p 8080:8080 ghga/ghga-connector:0.2.13 --help
```

If you prefer not to use containers, you may install the service from source:
```bash
# Execute in the repo's root dir:
pip install .

# To run the service:
ghga_connector --help
```

## Configuration
### Parameters

The service requires the following configuration parameters:
- **`upload_api`** *(string)*: URL to the root of the upload controller API. Default: `https://hd-dev.ghga-dev.de/ucs`.

- **`download_api`** *(string)*: URL to the root of the DRS-compatible API used for download. Default: `https://hd-dev.ghga-dev.de/drs3/ga4gh/drs/v1`.

- **`max_retries`** *(integer)*: Number of times to retry failed API calls. Default: `5`.

- **`max_wait_time`** *(integer)*: Maximal time in seconds to wait before quitting without a download. Default: `3600`.

- **`part_size`** *(integer)*: The part size to use for download. Default: `16777216`.

- **`server_pubkey`** *(string)*: Base64 encoded current GHGA public key for Crypt4GH encryption.

- **`wps_api_url`** *(string)*: URL to the root of the WPS API.


### Usage:

A template YAML for configurating the service can be found at
[`./example-config.yaml`](./example-config.yaml).
Please adapt it, rename it to `.ghga_connector.yaml`, and place it into one of the following locations:
- in the current working directory were you are execute the service (on unix: `./.ghga_connector.yaml`)
- in your home directory (on unix: `~/.ghga_connector.yaml`)

The config yaml will be automatically parsed by the service.

**Important: If you are using containers, the locations refer to paths within the container.**

All parameters mentioned in the [`./example-config.yaml`](./example-config.yaml)
could also be set using environment variables or file secrets.

For naming the environment variables, just prefix the parameter name with `ghga_connector_`,
e.g. for the `host` set an environment variable named `ghga_connector_host`
(you may use both upper or lower cases, however, it is standard to define all env
variables in upper cases).

To using file secrets please refer to the
[corresponding section](https://pydantic-docs.helpmanual.io/usage/settings/#secret-support)
of the pydantic documentation.



## Architecture and Design:
<!-- Please provide an overview of the architecture and design of the code base.
Mention anything that deviates from the standard triple hexagonal architecture and
the corresponding structure. -->

This is a Python-based service following the Triple Hexagonal Architecture pattern.
It uses protocol/provider pairs and dependency injection mechanisms provided by the
[hexkit](https://github.com/ghga-de/hexkit) library.


## Development
For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of vscode
in combination with Docker Compose.

To use it, you have to have Docker Compose as well as vscode with its "Remote - Containers"
extension (`ms-vscode-remote.remote-containers`) installed.
Then open this repository in vscode and run the command
`Remote-Containers: Reopen in Container` from the vscode "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant vscode extensions pre-installed
- pre-configured linting and auto-formating
- a pre-configured debugger
- automatic license-header insertion

Moreover, inside the devcontainer, a convenience commands `dev_install` is available.
It installs the service with all development dependencies, installs pre-commit.

The installation is performed automatically when you build the devcontainer. However,
if you update dependencies in the [`./setup.cfg`](./setup.cfg) or the
[`./requirements-dev.txt`](./requirements-dev.txt), please run it again.

## License
This repository is free to use and modify according to the
[Apache 2.0 License](./LICENSE).

## Readme Generation
This readme is autogenerate, please see [`readme_generation.md`](./readme_generation.md)
for details.
