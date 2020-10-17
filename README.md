# dng-mdk

This CLI tool allows you to export all the requirements from a Doors NG project into a self-contained RDF file. This tool also provides a command that will subsequently convert that exported project into a SysML model which can be uploaded to MMS.

All schema-related information about the project, such as the names and object-type/datatype ranges of custom properties, are handled appropriately when creating the SysML model. This ensures data quality and model consistency no matter what type of project is exported, as long as the data conforms to the OSLC vocabulary (for Doors NG, this will always be the case).

## Requirements
Node.js >= v14.13.0

## Install

```console
$ npm i
```

## CLI

For more info about a particular command, use `./dng-mdk COMMAND --help` .

All commands require the cli option `--mopid`, which is for specifying the destination MMS Org and Project ID as such: `org/project-id`.

```console
$ ./dng-mdk --help
dng-mdk <command>

Commands:
  dng-mdk export            Export a project from DNG
  dng-mdk translate         Translate an exported DNG project from RDF into another format
  dng-mdk upload            Upload a project to MMS

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
export DNG_USER=user
export DNG_PASS=pass
```

Then, simply `$ source .env` before running the CLI.


### Export

Given a project, download all of its artifacts and the associated property definitions as RDF.

The `./dng-mdk export` command will run the exporter and save the Turtle outuput under `data/`:
```console
$ ./dng-mdk export --project 'Full DNG Project Name' --mopid {MMS_ORG}/{MMS_PROJECT_ID}
```

The file will be symlinked as `data/{MMS_ORG}/{MMS_PROJECT_ID}/exported` .


### Translate

Once an RDF snapshot has been exported from DNG, you can 'translate' the project to another format, which may or may not be a lossy transformation depending on the target.

```console
$ ./dng-mdk translate --target mms --mopid {MMS_ORG}/{MMS_PROJECT_ID}
```


## Upload

Upload translated formats to their destinations.

```console
$ ./dng-mdk upload --mopid {MMS_ORG}/{MMS_PROJECT_ID}
```
