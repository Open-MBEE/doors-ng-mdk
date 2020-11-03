# dng-mdk

This CLI tool allows you to export all the requirements from a Doors NG project and load them into MMS.

All schema-related information about the project, such as the names and object-type/datatype ranges of custom properties, are handled appropriately when creating the SysML model. This ensures data quality and model consistency no matter what type of project is exported, as long as the data conforms to the OSLC vocabulary (for Doors NG, this will always be the case).

## Requirements
Node.js >= v14.13.0

## Install

From the project's root directory:
```console
$ npm install
```

To link the CLI, you can use:
```console
$ npm link
```

If running on a personal machine, it is suggested to [set your npm prefix](https://stackoverflow.com/a/23889603/14284216) so that the CLI is not linked globally.

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

For local testing, it is recommended that your create a `.env` file with all the enviornment variables:
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

### Sync

This command will automatically create and update an MMS project based on a DNG project matching the provided project name. It will load baselines, compute deltas, commit tags, and update the latest master branch.

Say we have a project on Doors NG entitled "Example Test", and we want to sync it with a project on MMS `test` under the `eg` org. We'll allocate 24 GiB of memory to ensure ample space for the program to compute deltas between model versions:
```console
$ dng-mdk sync eg/test --project 'Example Test' --malloc 24576 &> eg-test.log
```

### Docker
To build the dng-mdk docker image:
```console
$ docker build --build-args dng_server {DNG_SERVER} --build-args mms_server {MMS_SERVER} -t dng-mdk:latest .
```
To runL
```console
$ docker run -e "DNG_USER={DNG_USER}" -e "DNG_PASS={DNG_PASS}" dng-mdk:latest export {DNG_PROJECT}
```
