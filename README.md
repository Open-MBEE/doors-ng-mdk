# dng-mdk

This CLI tool allows you to export all the requirements from a Doors NG project and load them into MMS.

All schema-related information about the project, such as the names and object-type/datatype ranges of custom properties, are handled appropriately when creating the SysML model. This ensures data quality and model consistency no matter what type of project is exported, as long as the data conforms to the OSLC vocabulary (for Doors NG, this will always be the case).

## Contents
 - [Getting Started](#getting-started)
 - Install
   - [from Docker Hub](#install-from-docker-hub)
   - [from NPM](#install-from-npm)
   - [from source](#install-from-source)
     - [build docker image](#build-docker-image)
 - [CLI Usage](#cli)
   - [`sync` command](#cli-sync)
   - [`trigger` command](#cli-trigger)

## Getting Started

There are several ways to get started using this tool. The best approach for most cases is to simply [use the pre-built docker image available from Docker Hub](#install-from-docker-hub).


## Install from Docker Hub

Running this tool as a docker container is the simplest method for getting started.

**Requirements:**
 - [Docker](https://www.docker.com/get-started)

**Install:**
```console
$ docker pull openmbee/dng-mdk:latest
```

**Prepare:**
Create a file to store configuration and user credentials that the tool will use to connect to Doors NG and MMS:

For example, in a file called `.docker-env`
```bash
DNG_SERVER=https://jazz.xyz.org
DNG_USER=user
DNG_PASS=pass
MMS_SERVER=https://mms.xyz.org
MMS_USER=user
MMS_PASS=pass
```

**Run:**
```console
$ docker run -it --init --rm --env-file .docker-env openmbee/dng-mdk:latest sync --help
```

The above shell command will print the help message for the `sync` command.

The `-it --init` options will allow you to interactively cancel and close the command while it is running through your terminal.

The `--rm` option will remove the stopped container from your file system once it exits.

The `--env-file .docker-env` option points docker to your environments variables file.


## Install from NPM

This approach has less overhead than running as a docker container, but may require more setup.

**Requirements:**
 - Node.js >= v14.13.0

> If running on a personal machine and you do not already have Node.js installed, `webi` is the recommended install method since it will automatically configure node and npm for you:
[https://webinstall.dev/node/](https://webinstall.dev/node/)


Install the package globally:

```console
$ npm install -g dng-mdk
```

Confirm the CLI is linked:

```console
$ dng-mdk --version
```

If the above works, congrats! You're good to go.

However, if you got an error, it is likely that your npm has not yet been configured on where to put global packages.

For Linux and MacOS:
```
$ mkdir ~/.npm-global
$ echo -e "export NPM_CONFIG_PREFIX=~/.npm-global\nexport PATH=\$PATH:~/.npm-global/bin" >> ~/.bashrc
$ source ~/.bashrc
```


## Install from source

This approach is for developers who wish to edit the source code for testing changes.

From the project's root directory:
```console
$ npm install
```

To link the CLI, you can use:
```console
$ npm link
```

If running on a personal machine, it is suggested to [set your npm prefix](https://stackoverflow.com/a/23889603/14284216) so that the CLI is not linked globally.


### Build Docker image

To build the dng-mdk docker image locally:
```console
$ docker build --build-args dng_server {DNG_SERVER} --build-args mms_server {MMS_SERVER} -t dng-mdk .
```

To run:
```console
$ docker run -e "DNG_USER={DNG_USER}" -e "DNG_PASS={DNG_PASS}" dng-mdk sync --help
```


## CLI

For more info about a particular command, use `dng-mdk COMMAND --help` .

All commands require the positional argument `MMS_ORG_PROJECT_ID`, which is for specifying the destination MMS Org and Project ID as such: `org/project-id`.

```console
dng-mdk <command>

Commands:
  dng-mdk sync <MMS_ORG_PROJECT_ID>     Sync a DNG project with MMS
  dng-mdk trigger <MMS_ORG_PROJECT_ID>  Trigger a job

Options:
  --version  Show version number                                       [boolean]
  --help     Show help                                                 [boolean]

Environment Variables:
  DNG_SERVER      URL for DNG server
  DNG_USER        Username for DNG auth
  DNG_PASS        Password for DNG auth
  MMS_SERVER      URL for MMS server
  MMS_USER        Username for MMS auth
  MMS_PASS        Password for MMS auth
```

For local testing, it is recommended that your create a `.env` file with all the enviornment variables (docker users skip this step):

For Linux and MacOS:
```bash
#!/bin/bash
export DNG_SERVER=https://jazz.xyz.org
export DNG_USER=user
export DNG_PASS=pass
export MMS_SERVER=https://mms.xyz.org
export MMS_USER=user
export MMS_PASS=pass
```

Then, simply `$ source .env` before running the CLI.

For Windows:
```powershell
set DNG_SERVER=https://jazz.xyz.org
set DNG_USER=user
set DNG_PASS=pass
set MMS_SERVER=https://mms.xyz.org
set MMS_USER=user
set MMS_PASS=pass
```

### CLI: Sync

Use `dng-mdk sync --help` for the latest documentation about this command's options.

This command will automatically create and update an MMS project based on a DNG project matching the provided project name. It will load baselines, compute deltas, commit tags, and update the latest master branch.

Say we have a project on Doors NG entitled "Example Test", and we want to sync it with a project on MMS `test` under the `eg` org. We'll allocate 24 GiB of memory to ensure ample space for the program to compute deltas between model versions:
```console
$ dng-mdk sync eg/test --project 'Example Test' --malloc 24576 &> eg-test.log
```

> Note: This is just an example for processing a very large project; smaller projects will work fine with the default 1.4 GiB if you omit the `--malloc` option.

### CLI: Trigger

Use `dng-mdk trigger --help` for the latest documentation about this command's options.

This command is for triggering downstream tasks to update external services using the DNG projects stored in MMS. Right now, the only supported job is for triggering the IncQuery indexing services.

```console
$ dng-mdk trigger eg/test --job incquery --server https://incquery.xyz.org
```

