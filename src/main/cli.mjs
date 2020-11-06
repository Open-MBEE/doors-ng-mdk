#!/usr/bin/env node
/* eslint-disable no-console, quote-props */
import fs from 'fs';
import os from 'os';
import path from 'path';
import {URL, URLSearchParams} from 'url';
import {fork} from 'child_process';
import {filename} from 'dirname-filename-esm';
import {once} from 'events';
import {pipeline} from 'stream';

import StreamJson from '../util/stream-json.js';
const {
	JsonPick,
	JsonStreamArray,
} = StreamJson;

import yargs from 'yargs';
import {hideBin} from 'yargs/helpers';
import pino from 'pino';
const logger = pino();

import chalk from 'chalk';
const cherr = chalk.stderr;

import {
	file_exists,
	hash,
	request,
	fetch,
	upload,
} from '../util/io.mjs';

import {
	DngProject,
} from '../class/dng-project.mjs';

import {
	MmsProject,
} from '../class/mms-project.mjs';

import dng_translate from '../dng/translate.mjs';

// cli error handling
process.on('unhandledRejection', (e_fatal) => {
	console.error(e_fatal);
	process.exit(1);
});

// root dir
// const pd_root = path.join(os.tmpdir(), 'dng-mdk');
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

const load_mms_project_json = pr_file => new Promise((fk_resolve, fe_reject) => {
	const h_elements = {};

	const ds_pipeline = pipeline([
		fs.createReadStream(pr_file),
		JsonPick.withParser({filter:'elements'}),
		new JsonStreamArray(),
	], (e_pipe) => {
		if(e_pipe) {
			fe_reject(new Error(`Error while stream parsing project JSON from file ${pr_file}: ${e_pipe.stack}`));
		}
		else {
			fk_resolve(h_elements);
		}
	});

	ds_pipeline.on('data', ({value:g_element}) => {
		h_elements[g_element.id] = g_element;
	});
});


async function baseline_json_path(k_dng, g_baseline, gc_action) {
	// serialized project
	const p_json = `./baselines/mms-full.${g_baseline.id}.json`;

	// project was not yet exported
	if(!file_exists(p_json)) {
		// tmp rdf file
		const p_export = `./baselines/${g_baseline.id}.ttl`;

		if(!file_exists(p_export)) {
			// download dng project as RDF dataset to disk
			await k_dng.export({
				...gc_action,
				dng_context: g_baseline.uri,
				local_output: fs.createWriteStream(p_export),
			});
		}

		// translate dataset into MMS UML+JSON
		await dng_translate({
			...gc_action,
			local_exported: fs.createReadStream(p_export),
			local_adds: fs.createWriteStream(p_json),
		});
	}

	// now load into memory
	return p_json;
}

async function load_baseline(k_dng, g_baseline, gc_action) {
	const p_json = await baseline_json_path(k_dng, g_baseline, gc_action);
	return await load_mms_project_json(p_json);
}

// parse CLI args
let y_yargs = yargs(hideBin(process.argv))
	.usage('dng-mdk <command>');

// 'sync' command
y_yargs = y_yargs.command({
	command: 'sync <MMS_ORG_PROJECT_ID>',
	describe: 'Sync a DNG project with MMS',
	builder: _yargs => _yargs
		.usage('dng-mdk sync MMS_ORG_PROJECT_ID --project <DNG_PROJECT_NAME> [OPTIONS]')
		.positional('MMS_ORG_PROJECT_ID', {
			type: 'string',
			describe: 'org/project-id target for MMS project',
		})
		.options({
			project: {
				describe: 'full name of the project on DNG, case-sensitive',
				demandOption: true,
				type: 'string',
			},
			malloc: {
				describe: 'amount of memory to allocate for V8 instance in MiB',
				type: 'number',
			},
			sockets: {
				describe: 'number of sockets to use',
				type: 'number',
			},
			requests: {
				describe: 'number of concurrent requests to open',
				type: 'number',
			},
			reset: {
				type: 'boolean',
				describe: 'delete the project if it already exists on MMS and create a new project in its place ',
			},
			baselines: {
				type: 'number',
				describe: 'only sync the N latest baselines at most',
			},
			'auth-retries': {
				type: 'number',
				describe: 'number of times to retry failed DNG authentication',
			},
			'use-folders': {
				describe: `use the 'folders' workaround to fetch all artifacts for a large project`,
				type: 'boolean',
			},
		})
		.help().version(false),
	async handler(g_argv) {
		// malloc
		if(g_argv.malloc) {
			let a_args = [g_argv.MMS_ORG_PROJECT_ID];
			if(g_argv.project) a_args.push(...['--project', g_argv.project+'']);
			if(g_argv.sockets) a_args.push(...['--sockets', g_argv.sockets+'']);
			if(g_argv.requests) a_args.push(...['--requests', g_argv.requests+'']);
			if(g_argv.reset) a_args.push(...['--reset']);
			if(g_argv.baselines) a_args.push(...['--baselines', g_argv.baselines+'']);
			if(g_argv.authRetries) a_args.push(...['--auth-retries', g_argv.authRetries+'']);
			if(g_argv.useFolders) a_args.push(...['--use-folders']);

			const u_sub = fork(filename(import.meta), ['sync', ...a_args], {
				cwd: pd_root,
				execArgv: ['--max-old-space-size='+g_argv.malloc],
				stdio: 'inherit',
			});

			const xc_exit = await once(u_sub, 'close');
			process.exit(xc_exit);
		}

		const [pd_project, si_mms_project, si_mms_org] = project_dir(g_argv.MMS_ORG_PROJECT_ID);

		// dng server
		let p_server_dng = H_ENV.DNG_SERVER;
		{
			if(!p_server_dng) {
				throw new Error(`Must provide a DNG server URL via env var 'DNG_SERVER'`);
			}
			p_server_dng = (new URL(p_server_dng)).origin;

			// dng creds
			if(!H_ENV.DNG_USER || !H_ENV.DNG_PASS) {
				throw new Error(`Missing one of or both required environment variables: 'DNG_USER', 'DNG_PASS`);
			}
		}

		// mkdir -p ./data/{org}/{project}/
		fs.mkdirSync(pd_project, {recursive:true});

		// pushd
		process.chdir(pd_project);

		// prep action config
		const gc_action = {
			dng_server: p_server_dng,
			dng_project_name: g_argv.project,
			dng_use_folders: g_argv.useFolders,
			dng_auth_retries: g_argv.authRetries || 3,
			local_project_dir: pd_project,
			mms_server: H_ENV.MMS_SERVER,
			mms_project_org: si_mms_org,
			mms_project_id: si_mms_project,
			https_requests: g_argv.requests,
			https_sockets: g_argv.sockets,
		};

		// init dng project
		const k_dng = new DngProject(gc_action);

		// dng project info
		Object.assign(gc_action, await k_dng.info());

		// init mms project instance
		const k_mms = new MmsProject(gc_action);

		// ensure project exists, new one created
		await k_mms.create(g_argv.reset);

		// load refs
		const h_refs = await k_mms.refs();

		// mkdir ./baselines
		fs.mkdirSync('baselines', {recursive:true});

		// number of baselines specified or all baselines by default
		let n_max_baselines = g_argv.baselines;
		if(0 !== n_max_baselines) {
			// number safety
			if('number' !== n_max_baselines || Number.isNaN(n_max_baselines)) {
				n_max_baselines = 0;
			}

			// fetch dng baseline info
			const {
				histories: h_histories,
				map: h_baselines,
			} = await k_dng.fetch_baselines();

			// use default stream
			const a_history = Object.values(h_histories)[0];

			// optimization to speed up delta loading
			let g_previous;

			// each baseline in order
			const nl_baselines = a_history.length;
			const i_baseline_start = n_max_baselines? Math.min(0, nl_baselines-n_max_baselines): 0;
			for(let i_baseline=i_baseline_start; i_baseline<nl_baselines; i_baseline++) {
				const p_baseline = a_history[i_baseline];
				const g_baseline = h_baselines[p_baseline];

				// baseline already exists in MMS; skip it
				if(h_refs[hash(`baseline.${g_baseline.id}`)]) {
					console.warn(`skipping baseline which already exists in MMS: '${g_baseline.id}'`);
					continue;
				}

				// baseline has 'previous' dependency
				if(g_baseline.previous) {
					// load previous into memory
					let h_elements_previous;

					// previous already loaded into memory
					if(g_previous && g_previous.id === g_baseline.previous) {
						h_elements_previous = g_previous.elements;
					}
					// (down)load baseline
					else {
						h_elements_previous = await load_baseline(k_dng, h_baselines[g_baseline.previous], gc_action);
					}

					// load baseline into memory
					const h_elements_baseline = await load_baseline(k_dng, g_baseline, gc_action);

					// apply deltas
					await k_mms.apply_deltas(h_elements_previous, h_elements_baseline, 'master');

					// set previous
					g_previous = {
						id: g_baseline.id,
						elements: h_elements_baseline,
					};
				}
				// baseline has no previous
				else {
					// free 'previous' cache
					g_previous = null;

					// prep baseline elements
					const p_json_baseline = await baseline_json_path(k_dng, g_baseline, gc_action);

					// serialize in full to mms
					await k_mms.upload_json_stream(fs.createReadStream(p_json_baseline), 'master');
				}

				// tag current HEAD as baseline
				await k_mms.tag_head_as_baseline(g_baseline, 'master');
			}
		}
		// skip baselines
		else {
			console.warn(`skipping baselines`);
		}

		// finally, figure out difference between latest mms and latest dng
		{
			const si_latest = (new Date()).toISOString().replace(/[^A-Z0-9-_.]/g, '-');

			// load latest master from MMS
			const h_elements_mms = await k_mms.load('master');

			// load latest from DNG
			const h_elements_latest = await load_baseline(k_dng, {id:si_latest}, gc_action);

			// apply deltas
			await k_mms.apply_deltas(h_elements_mms, h_elements_latest, 'master');
		}
	},
});


// 'trigger' command
y_yargs = y_yargs.command({
	command: 'trigger <MMS_ORG_PROJECT_ID>',
	describe: 'Trigger a job',
	builder: _yargs => _yargs
		.usage('dng-mdk trigger MMS_ORG_PROJECT_ID --job <JOB> [OPTIONS]')
		.positional('MMS_ORG_PROJECT_ID', {
			type: 'string',
			describe: 'org/project-id target for MMS project',
		})
		.options({
			job: {
				describe: 'named job to run',
				type: 'string',
				demandOption: true,
			},
			server: {
				describe: 'the server URL',
				type: 'string',
				demandOption: true,
			},
		})
		.help().version(false),
	async handler(g_argv) {
		const {
			job: s_job,
			server: p_server,
		} = g_argv;

		const [, si_mms_project, si_mms_org] = project_dir(g_argv.MMS_ORG_PROJECT_ID);

		switch(s_job.toLowerCase()) {
			case 'incquery': {
				const h_headers_iqs = {
					...H_HEADERS_JSON,
					'Authorization': `Basic ${Buffer.from(H_ENV.MMS_USER+':'+H_ENV.MMS_PASS).toString('base64')}`,
				};

				console.warn(`refreshing repositories...`);

				// refresh mms repos
				const g_update = await upload(JSON.stringify({
					returnListOfNewCompartments: true,
				}), `${p_server}/api/mms-repository.update`, {
					method: 'POST',
					headers: h_headers_iqs,
				});

				// prep compartment start string
				const s_compartment_start = `mms-index:/orgs/${si_mms_org}/projects/${si_mms_project}/refs/master/commits/`;

				// list of commits
				const a_commits = [];

				// scan new model compartments
				for(const p_compartment of g_update.newModelCompartments) {
					// mopid match
					if(p_compartment.startsWith(s_compartment_start)) {
						a_commits.push(p_compartment);
					}
				}

				// prep compartment IRI
				let p_compartment;

				// most recent compartment IRI is present
				if(1 === a_commits.length) {
					p_compartment = a_commits[0];
				}
				// need to fetch from list
				else {
					console.warn(`scanning commits...`);
					console.time('scan');

					const g_body = await fetch(`${p_server}/api/mms-repository.info?`+(new URLSearchParams({
						returnAsListOfDescriptors: true,
					})), {
						method: 'GET',
						headers: h_headers_iqs,
					});

					let g_most_recent = null;

					COMMIT_SCAN:
					// each org
					for(const g_org of g_body.repositoryStructure.orgs) {
						// matching org
						if(si_mms_org === g_org.orgId) {
							// each project
							for(const g_project of g_org.projects) {
								// matching project
								if(si_mms_project === g_project.projectId) {
									// each ref
									for(const g_ref of g_project.refs) {
										// matching ref
										if('master' === g_ref.refId) {
											// each commit
											for(const g_commit of g_ref.commits) {
												// parse commit datetime
												const xt_commit = (new Date(g_commit.name)).getTime();

												// most recent
												if(!g_most_recent || xt_commit > g_commit.timestamp) {
													g_most_recent = {
														...g_commit,
														timestamp: xt_commit,
													};
												}
											}

											// done scanning
											break COMMIT_SCAN;
										}
									}
								}
							}
						}
					}

					// nothing was found
					if(!g_most_recent) throw new Error(`The requested org/project was not found on <${p_server}>`);

					console.warn(`selecting most recent: ${(new Date(g_most_recent.timestamp)).toISOString()}`);

					// set compartment IRI
					p_compartment = `${s_compartment_start}${g_most_recent.commitId}`;
				}

				console.timeEnd('scan');
				console.warn(`selected new compartment IRI: ${p_compartment}`);
				console.time('select');

				// fetch existing persistent indexes
				const g_body_pers = await fetch(`${p_server}/api/persistent-index.listModelCompartments`, {
					method: 'GET',
					headers: h_headers_iqs,
				});

				console.timeEnd('select');

				// iterate over existing indexed compartments
				for(const g_compartment of g_body_pers.persistedModelCompartments) {
					const p_compartment_old = g_compartment.compartmentURI;

					// compartment already indexed, do not redo
					if(p_compartment_old === p_compartment) {
						console.warn(`compartment '${p_compartment}' is already indexed.`);
						process.exit(0);
					}
				}

				console.warn(`loading new compartment into persistent index...`);
				console.time('persistent');

				// prep compartment URI payload
				const s_payload = JSON.stringify({
					compartmentURI: p_compartment,
				});

				// load persistent index
				await upload(s_payload, `${p_server}/api/persistent-index.indexModelCompartment`, {
					method: 'POST',
					headers: h_headers_iqs,
				});

				console.timeEnd('persistent');
				console.warn(`loading new compartment into in-memory index...`);
				console.time('in-memory');

				// load in-memory index
				await upload(s_payload, `${p_server}/api/inmemory-index.loadModelCompartment`, {
					method: 'POST',
					headers: h_headers_iqs,
				});

				console.timeEnd('in-memory');
				console.warn(`loading new compartment into elastic-search index...`);
				console.time('elastic-search');

				// load elastic-search index
				await upload(s_payload, `${p_server}/api/elastic-search-integration.loadModelCompartment`, {
					method: 'POST',
					headers: h_headers_iqs,
				});

				console.timeEnd('elastic-search');
				console.warn(`loading new compartment into neptune index...`);
				console.time('neptune');

				// load neptune index
				try {
					await upload(JSON.stringify({
						modelCompartment: {compartmentURI:p_compartment},
						format: 'RDF_TURTLE',
					}), `${p_server}/api/amazon-neptune-integration.loadModelCompartment`, {
						method: 'POST',
						headers: h_headers_iqs,
					});
				}
				catch(e_index) {
					console.error(`Failed to create Neptune index but continuing anyway... ${e_index.stack}`);
				}

				console.timeEnd('neptune');
				console.warn(`deleting old compartments...`);
				console.time('delete');

				// finally, delete all the old compartments
				for(const g_compartment of g_body_pers.persistedModelCompartments) {
					const p_compartment_old = g_compartment.compartmentURI;

					// mopid match
					if(p_compartment_old.startsWith(s_compartment_start) && p_compartment !== p_compartment_old) {
						// figure out which indices it is loaded into
						const g_status = await upload(JSON.stringify({
							compartmentURI: p_compartment_old,
						}), `${p_server}/api/demo.compartmentIndexStatus`, {
							method: 'POST',
							headers: h_headers_iqs,
						});

						// delete them
						await upload(JSON.stringify({
							modelCompartment: {compartmentURI:p_compartment_old},
							indexes: g_status.indices,
						}), `${p_server}/api/demo.deleteModelCompartment`, {
							method: 'POST',
							headers: h_headers_iqs,
						});
					}
				}

				console.timeEnd('delete');
				console.warn('done');

				break;
			}

			default: {
				throw new Error(`No such job '${s_job}'`);
			}
		}
	},
});

y_yargs.demandCommand(1, 1)  // eslint-disable-line no-unused-expressions
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
