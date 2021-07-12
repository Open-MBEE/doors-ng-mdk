import {URL, URLSearchParams} from 'url';
import {once} from 'events';
import TurtleWriter from '@graphy/content.ttl.write';
import DataFactory from '@graphy/core.data.factory';
import factory from '@graphy/core.data.factory';
import pino from 'pino';
import chalk from 'chalk';

const cherr = chalk.stderr;
const logger = pino(pino.destination({dest:2, sync:false}));

import {
	DngCrawler,
} from '../class/dng-crawler.mjs';

import {
	SimpleOslcClient,
	decontextualize_quad,
} from '../class/oslc-client.mjs';

import dng_folder from '../dng/folder.mjs';
import H_PREFIXES from '../common/prefixes.mjs';

const {
	c1,
	namedNode,
} = factory;

const P_DCT_TITLE = factory.c1('dct:title', H_PREFIXES).value;

const KT_RDF_TYPE = c1('a');
const KT_IBM_NAV_PARENT = c1('ibm_nav:parent', H_PREFIXES);
const KT_OSLC_CONFIG_COMPONENT = c1('oslc_config:component', H_PREFIXES);
const KT_OSLC_CONFIG_CONFIGURATIONS = c1('oslc_config:configurations', H_PREFIXES);
const KT_OSLC_RM_USES = c1('oslc_rm:uses', H_PREFIXES);
const KT_JAZZ_NAV_SUBFOLDERS = c1('jazz_nav:subfolders', H_PREFIXES);

const c1v = sc1 => c1(sc1, H_PREFIXES).concise();
const all = as => as? [...as]: [];
const first = as => all(as)[0];

const SV1_RDF_TYPE = c1v('a');
const SV1_RDFS_MEMBER = c1v('rdfs:member');
const SV1_DCT_TITLE = c1v('dct:title');
const SV1_DCT_CREATED = c1v('dct:created');
const SV1_DCT_CREATOR = c1v('dct:creator');
const SV1_DCT_IDENTIFIER = c1v('dct:identifier');
const SV1_DCT_DESCRIPTION = c1v('dct:description');
const SV1_OSLC_CONFIG_OVERRIDES = c1v('oslc_config:overrides');
const SV1_OSLC_CONFIG_PREVIOUS_BASELINE = c1v('oslc_config:previousBaseline');
const SV1_OSLC_CONFIG_BASELINE_OF_STREAM = c1v('oslc_config:baselineOfStream');
const SV1_OSLC_CONFIG_BASELINES = c1v('oslc_config:baselines');
const SV1_OSLC_CONFIG_STREAMS = c1v('oslc_config:streams');

const SV1_OSLC_CONFIG_BASELINE = c1v('oslc_config:Baseline');
const SV1_OSLC_CONFIG_STREAM = c1v('oslc_config:Stream');


function stream_baseline_histories(h_baselines, h_streams) {
	let g_root = null;
	for(const [p_baseline, g_baseline] of Object.entries(h_baselines)) {
		if(!g_baseline.previous) {
			if(g_root) {
				throw new Error(`Multiple root baselines: <${g_root.uri}> and <${g_baseline.uri}>`);
				// throw new Error(`Multiple root baselines for the same stream: <${g_root.uri}> and <${g_baseline.uri}>`);
			}

			g_root = g_baseline;
		}
	}

	if(!g_root) {
		console.warn(`No inherent root baseline found for streams ${Object.keys(h_streams).map(s => `<${s}>`).join(', ')}`);
		return Object.values(h_baselines)
			.sort((g_a, g_b) => (new Date(g_a)).created - (new Date(g_b)).created);
	}

	const p_stream = g_root.bos;

	// generate history
	const a_history = [];
	(function lineage(p_previous) {
		// commit to history
		a_history.push(p_previous);

		// scan through baselines
		const a_children = [];
		for(const [p_baseline, g_baseline] of Object.entries(h_baselines)) {
			// found child
			if(p_previous === g_baseline.previous && p_stream === g_baseline.bos) {
				a_children.push(g_baseline);

				// check datetimes
				const xt_parent = (new Date(h_baselines[p_previous].created)).getTime();
				const xt_created = (new Date(g_baseline.created)).getTime();
				if(xt_parent >= xt_created) {
					throw new Error(`Baseline <${p_baseline}> has a creation date ${g_baseline.created} that preceeds its parent <${p_previous}> of ${h_baselines[p_previous].created}`);
				}
			}
		}

		// exactly one child
		if(1 === a_children.length) {
			lineage(a_children[0].uri);
		}
		// no children
		else if(!a_children.length) {
			return;
		}
		// multiple children
		else {
			throw new Error(`Branching baselines not implemented; baseline <${p_previous}> has multiple children: ${a_children.map(g => g.id)}`);
		}
	})(g_root.uri);

	return {
		[p_stream]: a_history,
	};
}



export class DngProject {
	constructor(gc_dng) {
		this._p_server = (new URL(gc_dng.dng_server || process.env.DNG_SERVER)).origin;

		this._s_project_name = gc_dng.dng_project_name;
		this._n_auth_retries = gc_dng.dng_auth_retries || 0;
		this._n_crawl_depth = gc_dng.dng_crawl_depth || 3;
		this._a_modules = gc_dng.dng_modules || [];
		this._a_folders = gc_dng.dng_folders || [];
	}

	async info() {
		// simple client
		const k_client = new SimpleOslcClient();

		// update prefixes
		const h_prefixes = k_client._h_prefixes;

		// authenticate
		await k_client.authenticate(this._n_auth_retries);

		// collect root services
		const ds_root_services = await k_client.root_services();
		// each root service
		const h_projects = {};
		for await(let p_service of ds_root_services) {
			const ds_rm_service = await k_client.fetch(p_service);

			// each quad
			for await(let kq_service of ds_rm_service) {
				// dct:title
				if(P_DCT_TITLE === kq_service.predicate.value) {
					h_projects[kq_service.object.value] = kq_service.subject.value;
				}
			}
		}
		// project query url
		let s_name_project = this._s_project_name;

		// select project
		const p_project = h_projects[s_name_project];
		let si_project;

		// no such project
		if(!p_project) {
			throw new Error(`No such project named '${s_name_project}'. The projects found on DNG were: ${JSON.stringify(h_projects, null, '\t')}`);
		}

		// verbose
		console.warn(`'${s_name_project}': <${p_project}>`);

		// fetch
		const kd_project = await k_client.load(p_project);

		// grab components
		const as_components = new Set();
		for(const kq_comp of kd_project.match(null, KT_OSLC_CONFIG_COMPONENT, null)) {
			as_components.add(kq_comp.object.value);
		}

		// extract project id from URI
		const h_prefixes_out = Object.assign({}, h_prefixes);
		{
			const m_project_id = /^(.+)\/rm\/oslc_rm\/([^/]+)\/services.xml$/.exec(p_project);
			if(!m_project_id) {
				throw new Error(`There was a problem while trying to parse the project id from the service URI <${p_project}>`);
			}
			else {
				si_project = m_project_id[2];

				Object.assign(h_prefixes_out, {
					project_root: p_project.replace(/\/services.xml$/, '/'),
					project_team_area: `${this._p_server}/rm/process/project-areas/${si_project}/team-areas/`,
					project_component: `${this._p_server}/rm/cm/component/${si_project}/`,
				});
			}
		}

		// commit to self
		this._si_project = si_project;
		this._h_prefixes = h_prefixes_out;
		this._as_components = as_components;

		return {
			dng_project_id: si_project,
			dng_project_name: s_name_project,
			dng_prefixes: h_prefixes_out,
			dng_components: as_components,
			mem_project: kd_project,
		};
	}

	async export(gc_export) {
		const ds_out = gc_export.local_output;

		const H_ENV = process.env;
		if(!H_ENV.DNG_USER || !H_ENV.DNG_PASS) {
			throw new Error(`Missing one of or both required environment variables: 'DNG_USER', 'DNG_PASS`);
		}

		const n_socket_limit = gc_export.https_sockets || 64;
		const n_concurrent_requests = gc_export.https_requests || n_socket_limit;

		const y_client = new SimpleOslcClient({
			...gc_export,
			dng_server: this._p_server,
			dng_username: H_ENV.DNG_USER,
			dng_password: H_ENV.DNG_PASS,
			https_sockets: n_socket_limit,
			https_requests: n_concurrent_requests,
			mdk_verbosity: gc_export.mdk_verbosity || H_ENV.DNG_MDK_DEBUG,
		});


		// authenticate
		await y_client.authenticate(this._n_auth_retries);

		// project info
		const {
			dng_project_id: si_project,
			dng_prefixes: h_prefixes,
			mem_project: kd_project,
		}= await this.info();

		let p_query_project;
		let p_query_folder;
		let ds_scribe;
		SELECT_PROJECT: {
			// for writing rdf to output stream
			ds_scribe = new TurtleWriter({
				prefixes: h_prefixes,
			});

			ds_scribe.pipe(ds_out);

			// commit id given
			if(gc_export.commit_id) {
				const sc1_model = `mms-graph:Model.${gc_export.commit_id}`;
				ds_scribe.write({
					type: 'c3',
					value: {
						[DataFactory.comment()]: `@graph ${DataFactory.c1(sc1_model, H_PREFIXES).verbose()}`,
					},
				});
			}

			// select query capabilities
			const k_query_capabilities = kd_project.match(null, KT_RDF_TYPE, c1('oslc:QueryCapability', h_prefixes));

			// each query capability
			for await(const kt_quad of k_query_capabilities) {
				// select resource types
				const k_rtypes = kd_project.match(kt_quad.subject, c1('oslc:resourceType', h_prefixes));

				// prep requirements list
				const a_requirements = [];

				// prep folders list
				const a_folders = [];

				// each resource type quad
				for await(const kt_quad_rtype of k_rtypes) {
					// find by requirement/collection resource types
					const sc1_rtype = kt_quad_rtype.object.concise(h_prefixes);
					// console.log(sc1_rtype);
					switch(sc1_rtype) {
						case 'oslc_rm:Requirement':
						case 'oslc_rm:RequirementCollection': {
							a_requirements.push(kt_quad_rtype.subject);
							break;
						}
						case 'jazz_nav:folder': {
							a_folders.push(kt_quad_rtype.subject);
							break;
						}
						default: {
							continue;
						}
					}
				}

				// requirements capability
				if(a_requirements.length) {
					// debugger;
					const kt_capability = a_requirements[0];
					const k_query_bases = kd_project.match(kt_capability, c1('oslc:queryBase', h_prefixes), null);

					// get query base URL
					for await(const kt_base of k_query_bases) {
						p_query_project = kt_base.object.value;
					}
				}

				// folders capability
				if(a_folders.length) {
					const kt_capability = a_folders[0];
					const k_query_bases = kd_project.match(kt_capability, c1('oslc:queryBase', h_prefixes), null);
					// debugger;

					// get query base URL
					for await(const kt_base of k_query_bases) {
						p_query_folder = kt_base.object.value;
					}
				}
			}
		}

		// modules
		const a_modules = this._a_modules;

		// folders
		const a_folders = this._a_folders;

		// gather requirements
		const as_requirements = new Set();
		GATHER_REQUIREMENTS: {
			// module selection
			if(a_modules.length) {
				// simple client
				const k_client = new SimpleOslcClient();

				// authenticate
				await k_client.authenticate(this._n_auth_retries);

				// each module
				for(const p_module of a_modules) {
					// load module
					const kd_module = await k_client.load((new URL(p_module))+'');

					// select artifacts
					const kd_artifacts = kd_module.match(namedNode(p_module), KT_OSLC_RM_USES, null);

					// each artifact, add to set
					for(const g_quad of kd_artifacts) {
						as_requirements.add(g_quad.object.value);
					}
				}

				// verbose
				console.warn(`${as_requirements.size} requirements gathered from ${a_modules.length} module(s)`);
			}
			// use folders
			else if(a_folders.length || gc_export.dng_use_folders) {
				const as_visited = new Set();

				// recursively gather requirments using folder workaround
				const f_recurse = async function recurse(p_folder, a_path=[]) {
					if(as_visited.has(p_folder)) return;
					as_visited.add(p_folder);

					// folder path
					const s_path = '/'+a_path.map(s => s.replace(/\//g, '\\/')).join('/');

					// gather requirements
					let c_discovered = 0;
					const a_add = await dng_folder(y_client, si_project, p_folder, 50e3);
					for(const p_requirement of a_add) {
						if(!as_requirements.has(p_requirement)) {
							as_requirements.add(p_requirement);
							c_discovered += 1;
						}
					}
					console.warn(`${c_discovered} new requirements discovered in ${s_path}`);

					// fetch child folders
					const kd_folder = await y_client.load(`${p_folder}?${new URLSearchParams({
						childFolders: 1,
					})}`);

					// select only children
					const kd_subfolders = kd_folder.match(null, KT_IBM_NAV_PARENT, namedNode(p_folder));

					// verbose
					console.warn(`recursing on ${s_path} folder <${p_folder}>...`);

					// each subfolder
					for(const g_quad of kd_subfolders) {
						const kt_subfolder = g_quad.subject;
						const p_subfolder = kt_subfolder.value;

						// fetch title
						const as_titles = kd_folder._h_quad_tree['*'][kt_subfolder.concise()][SV1_DCT_TITLE];
						let s_title = as_titles? c1([...as_titles][0]).value: '(unlabeled)';

						// recurse
						await f_recurse(p_subfolder, [...a_path, s_title]);
					}
				};

				// select folders
				if(a_folders.length) {
					// each folder
					for(const si_folder of a_folders) {
						await f_recurse(`${this._p_server}/rm/folders/${si_folder}`);
					}

					// verbose
					console.warn(`${as_requirements.size} requirements gathered from ${a_folders.length} folder(s)`);
				}
				// all folders
				else {
					f_recurse(`${this._p_server}/rm/folders/${si_project}`);
				}
			}
			else {
				// parse target URL
				let d_target = new URL(p_query_project);
				const k_project = await y_client.load(d_target+'');

				// select all requirements
				const k_requirements = k_project.match(null, KT_RDF_TYPE, c1('oslc_rm:Requirement', h_prefixes));
				for(const kt_req of k_requirements) {
					as_requirements.add(kt_req.subject.value);
				}

				// verbose
				console.warn(`${as_requirements.size} requirements gathered`);
			}
		}

		// spawn crawler
		const y_crawler = new DngCrawler(y_client, n_concurrent_requests, !!(a_modules.length+a_folders.length), ds_scribe);

		// prep tasks
		const a_tasks = [];

		// each requirement
		for(const p_requirement of as_requirements) {
			// spawn crawler on this requirement
			a_tasks.push(y_crawler.spawn(p_requirement, this._n_crawl_depth));
		}

		// begin tasks
		await Promise.all(a_tasks);

		// close output
		ds_scribe.end();
		ds_out.end();

		await once(ds_out, 'finish');
	}

	async fetch_baselines() {
		// simple client
		const k_client = new SimpleOslcClient();

		// authenticate
		await k_client.authenticate(this._n_auth_retries);

		// deconstruct args
		const {
			_as_components: as_components,
			_si_project: si_project,
		} = this;

		// no components
		if(!as_components.size) throw new Error(`Project ${si_project} has no components`);

		// multiple components
		if(as_components.size > 1) throw new Error(`Project ${si_project} has multiple components`);

		// component IRI
		const p_component = first(as_components);
		const si_component = p_component.replace(/^.*\/([^/]+)$/, '$1');

		// fetch configs
		let p_configs;
		{
			const kd_component = await k_client.load(p_component);
			const kd_config = kd_component.match(null, KT_OSLC_CONFIG_CONFIGURATIONS, null);
			p_configs = first(kd_config).object.value;
		}

		// load configs
		const kd_configs = await k_client.load(p_configs);

		const h_baselines = {};
		const h_streams = {};

		let c_streams = 0;
		let c_streams_deleted = 0;
		let c_baselines = 0;

		const load_stream_or_baseline = async(sv1_config) => {
			const p_config = sv1_config.slice(1);

			// already loaded
			if(p_config in h_baselines || p_config in h_streams) {
				console.warn(`skipping already loaded config <${p_config}>`);
				return;
			}

			// load configurations
			let kd_config;
			try {
				kd_config = await k_client.load(p_config);
			}
			catch(e_load) {
				if(404 === e_load._nc_status) {
					console.warn(cherr.yellow(`<${p_config}> does not exist`));
					c_streams_deleted += 1;
				}
				else {
					throw e_load;
				}
				return;
			}

			// prep probs tree
			const hv2_probs_config = kd_config._h_quad_tree['*'][sv1_config];
			const as_types = hv2_probs_config[SV1_RDF_TYPE];
			const s_title_config = c1(first(hv2_probs_config[SV1_DCT_TITLE]), H_PREFIXES).value;

			const firstv = sc1_pred => c1(first(hv2_probs_config[c1v(sc1_pred)]), H_PREFIXES).value;
			const sv1_overrides = first(hv2_probs_config[SV1_OSLC_CONFIG_OVERRIDES]);
			const sv1_previous = first(hv2_probs_config[SV1_OSLC_CONFIG_PREVIOUS_BASELINE]);

			// baseline
			if(as_types.has(SV1_OSLC_CONFIG_BASELINE)) {
				// save baseline
				h_baselines[p_config] = {
					id: firstv(SV1_DCT_IDENTIFIER),
					uri: p_config,
					title: s_title_config,
					created: firstv(SV1_DCT_CREATED),
					creator: firstv(SV1_DCT_CREATOR),
					overrides: sv1_overrides? c1(sv1_overrides, H_PREFIXES).value: null,
					previous: sv1_previous? c1(sv1_previous, H_PREFIXES).value: null,
					streams: firstv(SV1_OSLC_CONFIG_STREAMS),
					description: firstv(SV1_DCT_DESCRIPTION),
					bos: firstv(SV1_OSLC_CONFIG_BASELINE_OF_STREAM),
				};

				console.warn(cherr.blue(`<${p_config}> Baseline: ${s_title_config}`));
				c_baselines += 1;
			}
			// streams
			else if(as_types.has(SV1_OSLC_CONFIG_STREAM)) {
				// save stream
				h_streams[p_config] = {
					id: firstv(SV1_DCT_IDENTIFIER),
					uri: p_config,
					title: s_title_config,
					created: firstv(SV1_DCT_CREATED),
					creator: firstv(SV1_DCT_CREATOR),
				};
				console.warn(cherr.green(`<${p_config}> Stream: ${s_title_config}`));
				c_streams += 1;

				// // loop through its baselines
				// const as_baselines = hv2_probs_config[SV1_OSLC_CONFIG_BASELINES];
				// for(const sv1_baseline of as_baselines) {
				// 	await load_stream_or_baseline(sv1_baseline);
				// }
			}
		};

		// load metadata for all baselines and streams
		for(const sv1_config of kd_configs._h_quad_tree['*']['>'+p_configs][SV1_RDFS_MEMBER]) {
			await load_stream_or_baseline(sv1_config);
		}
		console.warn(`${c_baselines} baselines; ${c_streams} streams; ${c_streams_deleted} deleted streams`);

		// no baselines
		if(!c_baselines) {
			return;
		}

		// produce history for default stream
		const h_histories = stream_baseline_histories(h_baselines, h_streams);

		return {
			histories: h_histories,
			map: h_baselines,
		};
	}
}
