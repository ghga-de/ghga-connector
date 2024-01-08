
[![tests](https://github.com/ghga-de/ghga-connector/actions/workflows/unit_and_int_tests.yaml/badge.svg)](https://github.com/ghga-de/ghga-connector/actions/workflows/unit_and_int_tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/ghga-connector/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/ghga-connector?branch=main)

# GHGA Connector

GHGA Connector - A CLI client application for interacting with the GHGA system.

---

## Description

The GHGA Connector is a command line client designed to facilitate interaction with the file storage infrastructure of GHGA.

### Download and Decrypt

Currently, it provides functionality for downloading files, with the capability to interact with the RESTful APIs exposed by the Download Controller Service (https://github.com/ghga-de/download-controller-service).

For downloading, the Connector interacts with encrypted files. These files, encrypted according to the Crypt4GH standard (https://www.ga4gh.org/news_item/crypt4gh-a-secure-method-for-sharing-human-genetic-data/), can be decrypted using the Connector's ``decrypt``` command. This command accepts a directory location as input. An optional output directory location can be provided, which will be created if it does not exist (defaulting to the current working directory if none is provided).

The Connector requires the submitter's private key, which should match the public key announced to GHGA. This key is crucial for file decryption during the download process. Furthermore, the ``decrypt``` command also requires the private key to decrypt the downloaded files.

### Upload (WIP)

The upload functionality is currently under development. Once available, it will allow users to upload files to the GHGA storage infrastructure. The process will involve the encryption of unencrypted files according to the Crypt4GH standard before uploading. This ensures the secure transmission and storage of sensitive data. Detailed documentation and guidelines for the upload process will be provided upon the release of this feature.

## Installation and Configuration

This package can be installed using pip:

```bash
pip install ghga-connector==0.3.15
```

A pre-build version is available at [docker hub](https://hub.docker.com/repository/docker/ghga/ghga-connector):

```bash
docker pull ghga/ghga-connector:0.3.15
```

### Configuration

The service requires the following configuration parameters:
- **`max_retries`** *(integer)*: Number of times to retry failed API calls. Default: `5`.

- **`max_wait_time`** *(integer)*: Maximal time in seconds to wait before quitting without a download. Default: `3600`.

- **`part_size`** *(integer)*: The part size to use for download. Default: `16777216`.

- **`wkvs_api_url`** *(string)*: URL to the root of the WKVS API. Should start with https://.


## Usage

An overview of all commands is provided using:
```bash
ghga-connector --help
```

### Download
The ``download`` command is used to download files. In order to download files, you must provide a download token, which contains both the download instructions and authentication details.

### Decrypt
The files you download are encrypted. To decrypt a file, please use the ``decrypt`` command.


## Development

### Configuration:

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

For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of VS Code
in combination with Docker Compose.

To use it, you have to have Docker Compose as well as VS Code with its "Remote - Containers"
extension (`ms-vscode-remote.remote-containers`) installed.
Then open this repository in VS Code and run the command
`Remote-Containers: Reopen in Container` from the VS Code "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant VS Code extensions pre-installed
- pre-configured linting and auto-formatting
- a pre-configured debugger
- automatic license-header insertion

Moreover, inside the devcontainer, a convenience commands `dev_install` is available.
It installs the service with all development dependencies, installs pre-commit.

The installation is performed automatically when you build the devcontainer. However,
if you update dependencies in the [`./pyproject.toml`](./pyproject.toml) or the
[`./requirements-dev.txt`](./requirements-dev.txt), please run it again.

## License

This repository is free to use and modify according to the
[Apache 2.0 License](./LICENSE).

## README Generation

This README file is auto-generated, please see [`readme_generation.md`](./readme_generation.md)
for details.
