#!/usr/bin/env node
/* eslint-disable no-unused-expressions, no-console, quote-props */
import fs from 'fs';
import path from 'path';
import {URL, URLSearchParams} from 'url';
import https from 'https';
import stream from 'stream';
import {fork} from 'child_process';
import {filename} from 'dirname-filename-esm';
import {once} from 'events';

import yargs from 'yargs';
import {hideBin} from 'yargs/helpers';
import chalk from 'chalk';
const cherr = chalk.stderr;

import dng_export from '../dng/export.mjs';
import dng_translate from '../dng/translate.mjs';
import dng_delta from '../dng/delta.mjs';

const SR_CACHED = './cached';
const SR_EXPORTED = './exported';
const SR_PROJECT_LABEL = './project-label';
const SR_MMS_ADD = './mms-add.json';
const SR_MMS_DELETE = './mms-delete.json';

// cli error handling
process.on('unhandledRejection', (e_fatal) => {
	console.error(e_fatal);
	process.exit(1);
});

// cwd
const pd_root = process.cwd();

// ref env
const H_ENV = process.env;

// headers for json exchange
const H_HEADERS_JSON = {
	'Accept': 'application/json',
	'Content-Type': 'application/json',
};

// parse mms org/project string
function project_dir(s_mms) {
	const m_mms = /^([\w.-]+)\/([\w.-]+)$/.exec(s_mms);
	if(!m_mms) {
		throw new Error(`invalid mms org/project qualifier: '${s_mms}'`);
	}

	// extract mms org/project
	const [, si_mms_org, si_mms_project] = m_mms;

	// project output dir
	return [path.join(pd_root, 'data', si_mms_org, si_mms_project), si_mms_project, si_mms_org];
}

// test if file exists
function file_exists(p_file) {
	// attempt to access file
	try {
		fs.accessSync(p_file, fs.constants.R_OK);
	}
	catch(e_access) {
		return false;
	}

	return true;
}

const request = (p_url, gc_request) => new Promise((fk_resolve) => {
	(https.request(p_url, gc_request, fk_resolve)).end();
});

const upload = (z_input, p_url, gc_request) => new Promise((fk_resolve, fe_reject) => {
	let dt_waiting;

	// open request
	const ds_upload = https.request(p_url, gc_request, async(ds_res) => {
		clearInterval(dt_waiting);

		const n_status = ds_res.statusCode;

		// download response body
		let s_body = '';
		for await(const s_chunk of ds_res) {
			s_body += s_chunk;
		}

		// good
		if(n_status >= 200 && n_status < 300) {
			// parse
			let g_json;
			try {
				g_json = JSON.parse(s_body);
			}
			catch(e_parse) {
				return fe_reject(new Error(`Response body is not valid json: '''\n${s_body}\n'''`));
			}

			// resolve
			return fk_resolve(g_json);
		}
		// bad
		else {
			return fe_reject(new Error(`Unexpected response status ${n_status} from <${p_url}> '${ds_res.statusMessage}'; response body: '''\n${s_body}\n'''. Request metadata: ${JSON.stringify(gc_request, null, '\t')}`));
		}
	});

	// submit payload
	if('string' === typeof z_input || z_input?.byteLength) {
		ds_upload.end(z_input);
	}
	// stream
	else {
		stream.pipeline([
			z_input,
			ds_upload,
		], (e_upload) => {
			if(e_upload) {
				fe_reject(e_upload);
			}
			else {
				console.warn(`Payload successfully uploaded to <${p_url}>`);
				const t_start = Date.now();
				dt_waiting = setInterval(() => {
					const xs_elapsed = Math.round((Date.now() - t_start) / 1000);
					console.warn(`${Math.floor(xs_elapsed / 60)}m${((xs_elapsed % 60)+'').padStart(2, '0')}s have elapsed and still waiting...`);
				}, 1000*60*5);  // every 5 minutes
			}
		});
	}
});

// parse CLI args
yargs(hideBin(process.argv))
	.usage('dng-mdk <command>')

	// export command
	.command({
		command: 'export',
		describe: 'Export a project from DNG',
		builder: _yargs => _yargs
			.usage('dng-mdk export --project <PROJECT_NAME> --mopid MMS_ORG_PROJECT_ID [OPTIONS]')
			.options({
				project: {
					describe: 'full name of the project on DNG',
					demandOption: true,
					type: 'string',
				},
				mopid: {
					describe: 'org/project-id target for MMS project',
					type: 'string',
					demandOption: true,
				},
				sockets: {
					describe: 'number of sockets to use',
					type: 'number',
				},
				requests: {
					describe: 'number of concurrent requests to open',
					type: 'number',
				},
				'use-folders': {
					describe: `use the 'folders' workaround to fetch all artifacts for a large project`,
					type: 'boolean',
				},
				tidy: {
					describe: 'delete older cache files',
					type: 'boolean',
				},
				verbose: {
					alias: 'v',
					describe: 'verbosity of output; 1=some, 2=all',
					type: 'number',
				},
			})
			.help().version(false),
		async handler(g_argv) {
			const [pd_project] = project_dir(g_argv.mopid);

			// dng server
			const p_server_dng = H_ENV.DNG_SERVER;
			if(!p_server_dng) {
				throw new Error(`Must provide a DNG server URL via env var 'DNG_SERVER'`);
			}

			// mkdir -p ./data/{org}/{project}/
			fs.mkdirSync(pd_project, {
				recursive: true,
			});

			// pushd
			process.chdir(pd_project);

			// previous cache file exists
			if(file_exists(SR_CACHED)) {
				// keep tidy; delete cached file
				if(g_argv.tidy) {
					const p_prev_cached_real = fs.readlinkSync(SR_CACHED);
					fs.unlinkSync(p_prev_cached_real);
				}

				// delete cache symlink
				fs.unlinkSync(SR_CACHED);
			}

			// previous exported file exists
			if(file_exists(SR_EXPORTED)) {
				// shift exported -> cached
				const p_exported_real = fs.readlinkSync(SR_EXPORTED);
				fs.symlinkSync(p_exported_real, path.basename(SR_CACHED));
				fs.unlinkSync(SR_EXPORTED);
			}

			// no label, create one
			if(!file_exists(SR_PROJECT_LABEL)) {
				fs.writeFileSync(SR_PROJECT_LABEL, g_argv.project);
			}

			// popd
			process.chdir(pd_root);

			// output file name
			const sr_export = (new Date()).toISOString().replace(/[^A-Z0-9-_.]/g, '-')+'.ttl';

			// run export
			await dng_export({
				...g_argv,
				server: p_server_dng,
				output: fs.createWriteStream(path.join(pd_project, sr_export)),
			});

			// make symlink
			process.chdir(pd_project);
			fs.symlinkSync(sr_export, path.basename(SR_EXPORTED));
		},
	})

	// translate a project
	.command({
		command: 'translate',
		describe: 'Translate an exported DNG project from RDF into another format',
		builder: _yargs => _yargs
			.usage('dng-mdk translate --target <TARGET> --mopid MMS_ORG_PROJECT_ID [OPTIONS]')
			.options({
				target: {
					describe: `target destination to translate for, e.g., 'mms'`,
					type: 'string',
					demandOption: true,
				},
				malloc: {
					describe: 'amount of memory to allocate for V8 instance in MiB',
					type: 'number',
				},
				mopid: {
					describe: 'org/project-id target for MMS project',
					type: 'string',
					demandOption: true,
				},
				force: {
					describe: `force produce the whole translation, i.e., don't compute delta`,
					type: 'boolean',
				},
			})
			.help().version(false),
		async handler(g_argv) {
			const [pd_project, si_mms_project] = project_dir(g_argv.mopid);

			// dng server
			const p_server_dng = H_ENV.DNG_SERVER;
			if(!p_server_dng) {
				throw new Error(`Must provide a DNG server URL via env var 'DNG_SERVER'`);
			}

			// malloc
			if(g_argv.malloc) {
				let a_args = [];
				if(g_argv.target) a_args.push(...['--target', g_argv.target+'']);
				if(g_argv.mopid) a_args.push(...['--mopid', g_argv.mopid+'']);
				if(g_argv.force) a_args.push(...['--force']);
				const u_sub = fork(filename(import.meta), ['translate', ...a_args], {
					cwd: pd_root,
					execArgv: ['--max-old-space-size='+g_argv.malloc],
					stdio: 'inherit',
				});

				const xc_exit = await once(u_sub, 'close');
				process.exit(xc_exit);
			}

			// pushd
			process.chdir(pd_project);

			// read exported file
			const ds_exported = fs.createReadStream(SR_EXPORTED);

			// prep add output
			const ds_mms_add = fs.createWriteStream(SR_MMS_ADD);

			// unlink 'delete' delta
			if(file_exists(SR_MMS_DELETE)) {
				fs.unlinkSync(SR_MMS_DELETE);
			}

			// read project label
			let s_project_label = si_mms_project;
			if(file_exists(SR_PROJECT_LABEL)) {
				s_project_label = fs.readFileSync(SR_PROJECT_LABEL, 'utf8');
			}

			// produce delta
			if(file_exists(SR_CACHED) && !g_argv.force) {
				const {
					added: a_added,
					deleted: a_deleted,
				} = await dng_delta({
					...g_argv,
					server: p_server_dng,
					project: si_mms_project,
					label: s_project_label,
					exported: ds_exported,
					cached: fs.createReadStream(SR_CACHED),
					adds: ds_mms_add,
					deletes: fs.createWriteStream(SR_MMS_DELETE),
				});

				// something was deleted
				if(a_deleted.length) {
					console.info(`${a_deleted.length} artifacts were deleted`);
				}
				// nothing was deleted
				else {
					console.info(`nothing was deleted`);
					fs.unlinkSync(SR_MMS_DELETE);
				}

				// something was added
				if(a_added.length) {
					console.info(`${a_added.length} artifacts were added`);
				}
				// nothing was added
				else {
					console.info(`nothing was added`);
					fs.unlinkSync(SR_MMS_ADD);
				}
			}
			// do full translation
			else {
				await dng_translate({
					...g_argv,
					server: p_server_dng,
					project: si_mms_project,
					label: s_project_label,
					exported: ds_exported,
					adds: ds_mms_add,
				});
			}
		},
	})

	// upload to mms
	.command({
		command: 'upload',
		describe: 'Upload a project to MMS',
		builder: _yargs => _yargs
			.usage('dng-mdk upload --mopid MMS_ORG_PROJECT_ID [OPTIONS]')
			.options({
				mopid: {
					describe: 'org/project-id target for MMS project',
					type: 'string',
					demandOption: true,
				},
				reset: {
					describe: 'completely reset the project on MMS',
					type: 'boolean',
				},
			})
			.help().version(false),
		async handler(g_argv) {
			const [pd_project, si_mms_project, si_mms_org] = project_dir(g_argv.mopid);

			// mms server
			if(!H_ENV.MMS_SERVER) {
				throw new Error(`Must provide an MMS server URL via env var 'MMS_SERVER'`);
			}
			if(!H_ENV.MMS_USER || !H_ENV.MMS_PASS) {
				throw new Error(`Must provide MMS user and pass via env vars 'MMS_USER', 'MMS_PASS'`);
			}
			const p_server_mms = (new URL(H_ENV.MMS_SERVER)).origin;

			// pushd
			process.chdir(pd_project);

			const s_ref = 'master';

			const h_headers_mms = {
				...H_HEADERS_JSON,
				'Authorization': `Basic ${Buffer.from(H_ENV.MMS_USER+':'+H_ENV.MMS_PASS).toString('base64')}`,
				'Transfer-Encoding': 'chunked',
			};

			const p_endpoint_service = `${p_server_mms}/alfresco/service`;
			const p_endpoint_project = `${p_endpoint_service}/projects/${si_mms_project}`;
			const p_endpoint_elements = `${p_endpoint_project}/refs/${s_ref}/elements`;

			// find out if project exists
			console.warn(`GET <${p_endpoint_project}>...`);
			const ds_res = await request(p_endpoint_project, {
				method: 'GET',
				headers: h_headers_mms,
			});

			// not exist
			if(404 === ds_res.statusCode) {
				// create
				console.warn(`${g_argv.mopid} does not yet exist on ${p_server_mms}; creating project...`);
				console.time('create');

				// read project label
				let s_project_label = si_mms_project;
				if(file_exists(SR_PROJECT_LABEL)) {
					s_project_label = fs.readFileSync(SR_PROJECT_LABEL, 'utf8');
				}

				// prep payload
				const s_payload = JSON.stringify({
					projects: [{
						type: 'Project',
						orgId: si_mms_org,
						id: si_mms_project,
						name: s_project_label.trim().replace(/\s+/g, ' '),
					}],
				});

				// create project
				await upload(s_payload, `${p_endpoint_service}/orgs/${si_mms_org}/projects`, {
					method: 'POST',
					headers: h_headers_mms,
				});

				console.timeEnd('create');
			}
			// reset
			else if(g_argv.reset) {
				console.log(`DELETE project from ${p_endpoint_project}...`);
				console.time('reset');

				// submit request
				await request(p_endpoint_project, {
					method: 'DELETE',
					headers: h_headers_mms,
				});

				console.timeEnd('reset');
			}
			// deletes
			else if(file_exists(SR_MMS_DELETE)) {
				console.log(`DELETE ${SR_MMS_DELETE} from ${p_endpoint_elements}...`);
				console.time('delete');

				// submit request
				await upload(fs.createReadStream(SR_MMS_DELETE), p_endpoint_elements, {
					method: 'DELETE',
					headers: h_headers_mms,
				});

				console.timeEnd('delete');
			}

			// adds
			if(file_exists(SR_MMS_ADD)) {
				const p_endpoint_elements_add = `${p_endpoint_elements}?${new URLSearchParams({
					overwrite: true,
				})}`;

				console.log(`POST ${SR_MMS_ADD} to ${p_endpoint_elements}...`);
				console.time('add');

				// submit request
				await upload(fs.createReadStream(SR_MMS_ADD), p_endpoint_elements_add, {
					method: 'POST',
					headers: h_headers_mms,
				});

				console.timeEnd('add');
			}
		},
	})

	// trigger
	.command({
		command: 'trigger',
		describe: 'Trigger a job',
		builder: _yargs => _yargs
			.usage('dng-mdk trigger --job <JOB> --mopid MMS_ORG_PROJECT_ID [OPTIONS]')
			.options({
				mopid: {
					describe: 'org/project-id target for MMS project',
					type: 'string',
					demandOption: true,
				},
				job: {
					describe: 'named job to run',
					type: 'string',
				},
				server: {
					describe: 'the server URL',
					type: 'string',
				},
			})
			.help().version(false),
		async handler(g_argv) {
			const {
				job: s_job,
				server: p_server,
			} = g_argv;

			const [, si_mms_project, si_mms_org] = project_dir(g_argv.mopid);

			switch(s_job.toLowerCase()) {
				case 'incquery': {
					// refresh mms repos
					const g_repos = await upload(JSON.stringify({
						returnListOfNewCompartments: true,
					}), `${p_server}/mms-repository.update`, {
						method: 'POST',
						headers: {
							...H_HEADERS_JSON,
							'Authorization': `Basic ${Buffer.from(H_ENV.MMS_USER+':'+H_ENV.MMS_PASS).toString('base64')}`,
						},
					});

					// select compartment URI
					debugger;


					break;
				}

				default: {
					throw new Error(`No such job '${s_job}'`);
				}
			}
		},
	})

	.demandCommand(1, 1)
	.help()
	.epilog(`Environment Variables:
		DNG_SERVER      URL for DNG server
		DNG_USER        Username for DNG auth
		DNG_PASS        Password for DNG auth
		MMS_SERVER      URL for MMS server
		MMS_USER        Username for MMS auth
		MMS_PASS        Password for MMS auth
	`.replace(/\n[ \t]+/g, '\n  '))
	.argv;
