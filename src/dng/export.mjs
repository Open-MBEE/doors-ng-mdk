import {URL, URLSearchParams} from 'url';
import cli_select from 'cli-select';
import TurtleWriter from '@graphy/content.ttl.write';
import FastDataset from '@graphy/memory.dataset.fast';
import factory from '@graphy/core.data.factory';
import pino from 'pino';
import chalk from 'chalk';

const cherr = chalk.stderr;
const logger = pino(pino.destination({dest:2, sync:false}));

import SimpleOslcClient from '../class/oslc-client.mjs';
import AsyncLockPool from '../util/async-lock-pool.mjs';
import dng_folder from './folder.mjs';
import H_PREFIXES from '../common/prefixes.mjs';
import {
	SkipError,
	HttpError,
} from '../common/errors.mjs';

const {
	c1,
	namedNode,
} = factory;

const A_NETWORK_RETRY_ERROR_CODES = [
	'ECONNRESET',
	'EPIPE',
	'ETIMEDOUT',
	'ENOTFOUND',
];

const P_DCT_TITLE = factory.c1('dct:title', H_PREFIXES).value;
const P_OSLC_INSTANCE_SHAPE = factory.c1('oslc:instanceShape', H_PREFIXES).value;

const KT_RDF_TYPE = c1('a');
const KT_IBM_FOLDER = c1('ibm_public:Folder', H_PREFIXES);
const KT_IBM_NAV_PARENT = c1('ibm_nav:parent', H_PREFIXES);
const KT_OLSC_CONFIG_COMPONENT = c1('oslc_config:component', H_PREFIXES);

const c1v = sc1 => c1(sc1, H_PREFIXES).concise();

const SV1_DCT_TITLE = c1v('dct:title');

const A_PATH_PREFIX_BLACKLIST = [
	'/rm/calmFilter/',
	'/rm/requirementFactory',
	'/rm/delivery-sessions',
	'/rm/reqif_oslc/',
	'/rm/type-import-sessions',
	'/rm/views?oslc.query',
	'/rm/accessControl/',
	'/rm/web',
	'/rm/pickers/',
	'/rm/folders/null',
	'/jts/users/photo/',
];

const A_RAW_BLACKLIST = [];

const RT_BLACKLIST = new RegExp(''
	+`^(${A_PATH_PREFIX_BLACKLIST
		.map(s => s.replace(/([/.[\]?\-+*$()|])/g, '\\$1'))
		.join('|')})`
	+(A_RAW_BLACKLIST.length
		? `|${A_RAW_BLACKLIST.join('|')}`
		:''));

class Crawler {
	constructor(y_client, n_concurrent_requests, ds_out=process.stdout) {
		this._y_client = y_client;
		this._k_dataset = FastDataset();
		this._h_cached = {};
		this._h_notified = {};
		this._p_origin = (new URL(y_client._p_server)).origin;
		this._ds_out = ds_out;
		this._k_pool = new AsyncLockPool(n_concurrent_requests);
		this._p_dng_custom_type = `${this._p_origin}/rm/types/`;
		this._h_prefixes_custom = {};
	}

	_handle_node_in(s_role, kt_quad, as_links, h_remap) {
		const kt_term = kt_quad[s_role];

		// add incoming and outgoing links to set
		if(kt_term.isNamedNode) {
			// ref term IRI
			const p_term = kt_term.value;

			// first encounter w/ node
			if(!as_links.has(p_term)) {
				// add to link set
				as_links.add(p_term);

				// IRI indicates custom DNG type; create prefix
				const p_dng_custom_type = this._p_dng_custom_type;
				let i_frag;
				if(p_term.startsWith(p_dng_custom_type) && -1 !== (i_frag=p_term.indexOf('#'))) {
					const si_prefix = `dng_type_${p_term.slice(p_dng_custom_type.length, i_frag)}`;

					if(!si_prefix.includes('/')) {
						if(!(si_prefix in this._h_prefixes_custom)) {
							this._ds_out.write({
								type: 'prefixes',
								value: {
									[si_prefix]: p_term.slice(0, i_frag+1),
								},
							});
							this._h_prefixes_custom[si_prefix] = 1;
						}
					}
				}
			}
		}
		// blank node
		else if(kt_term.isBlankNode) {
			const si_label = kt_term.value;

			// already mapped; use existing
			if(si_label in h_remap) {
				kt_quad[s_role] = h_remap[si_label];
			}
			// not yet mapped; create new unique blank node
			else {
				kt_quad[s_role] = h_remap[si_label] = factory.blankNode();
			}
		}
	}

	async spawn(sr_spawn, n_depth_max=Infinity, n_depth_cur=0, b_retry=false) {
		let d_url;
		try {
			d_url = new URL(sr_spawn, this._p_origin);
		}
		// invalid URL
		catch(e_parse) {
			console.warn(cherr.yellow(`skipping invalid URL: ${sr_spawn}`));
			return;
		}

		// IRI without fragment
		const p_url = d_url.toString().replace(/#.*$/, '');

		// not a retry attempt
		if(!b_retry) {
			// already visited this URI
			if(this._h_cached[p_url]) return;

			// now URI has been 'visted'
			this._h_cached[p_url] = 1;

			// different domain
			const p_origin = d_url.origin;
			if(p_origin !== this._p_origin) {
				if(!(p_origin in this._h_notified)) {
					console.warn(cherr.magenta(`skipping outside domain ${d_url.origin}`));
					this._h_notified[p_origin] = 1;
				}
				return;
			}

			// blacklisted
			if(RT_BLACKLIST.test(d_url.pathname)) {
				console.warn(cherr.magenta(`skipping blacklisted ${d_url.pathname}`));
				return;
			}
		}

		// ref prefixes
		const h_prefixes = this._y_client._h_prefixes;

		// acquire lock to use request
		const f_release = await this._k_pool.acquire(sr_spawn);

		// fetch
		let ds_rdf;
		try {
			ds_rdf = await this._y_client.fetch(p_url);
		}
		// network/http error
		catch(e_fetch) {
			// determine error type
			if(e_fetch instanceof HttpError) {
				console.error(cherr.red(e_fetch.message));
			}
			// skip
			else if(e_fetch instanceof SkipError) {
				// do nothing
			}
			// socket hang up
			else if(A_NETWORK_RETRY_ERROR_CODES.includes(e_fetch.code)) {
				console.warn(cherr.red(`${e_fetch.code} on '${sr_spawn}'; retrying...`));

				// release lock now
				f_release();

				// then make caller wait for resolve
				return new Promise((fk_resolve) => {
					// backoff a little
					setTimeout(async() => {
						// await retry
						await this.spawn(sr_spawn, n_depth_max, n_depth_cur, true);

						// resolve
						fk_resolve();
					}, 1500);
				});
			}
			else {
				debugger;
				throw e_fetch;
			}

			// release lock
			f_release();

			// do not continue
			return;
		}

		// success
		console.warn(cherr.green(`+${factory.namedNode(p_url).concise(h_prefixes)}`));

		// create local dataset
		const k_dataset = FastDataset({
			prefixes: h_prefixes,
		});

		// set of links to traverse
		const as_links = new Set();
		const as_must = new Set();

		// 
		const p_dng_prop_custom = this._p_dng_prop_custom;

		// remap blank nodes
		const h_remap = {};

		// load into local dataset
		for await(const kt_quad of ds_rdf) {
			// crawl incoming nodes
			this._handle_node_in('subject', kt_quad, as_links, h_remap);

			// ref predicate
			const p_predicate = kt_quad.predicate.value;

			// crawl predicate of requirement
			as_links.add(p_predicate);

			// must crawl object
			if(P_OSLC_INSTANCE_SHAPE === p_predicate) {
				this._handle_node_in('object', kt_quad, as_must, h_remap);
			}

			// crawl outgoing nodes
			this._handle_node_in('object', kt_quad, as_links, h_remap);

			// add (possibly mutated) quad to local dataset
			k_dataset.add(kt_quad);
		}

		// now that stream has been consumed
		{
			// close underlying resources
			ds_rdf.destroy();

			// release lock
			f_release();
		}


		// dump dataset to output
		this._ds_out.write({
			type: 'c3',
			value: k_dataset._h_quad_tree['*'],
		});

		// traverse links
		if(n_depth_cur < n_depth_max) {
			await Promise.all(Array.from(as_links).map((p_link) => {
				return this.spawn(p_link, n_depth_max, n_depth_cur+1);
			}));
		}
		// stopped, proceed with musts
		else {
			await Promise.all(Array.from(as_must).map((p_link) => {
				return this.spawn(p_link, n_depth_max, n_depth_cur+1);
			}));
		}
	}
}

export async function dng_project_info(gc_export) {
	const p_server = (new URL(gc_export.server || process.env.DNG_SERVER)).origin;

	// simple client
	const k_client = new SimpleOslcClient();

	// update prefixes
	const h_prefixes = k_client._h_prefixes;

	// authenticate
	await k_client.authenticate();

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
	let s_name_project = gc_export.project;

	// select project
	const p_project = h_projects[s_name_project];
	let si_project;

	// fetch
	const kd_project = await k_client.load(p_project);

	// verbose
	console.warn(`'${s_name_project}': <${p_project}>`);

	// grab components
	const as_components = new Set();
	for(const kq_comp of kd_project.match(null, KT_OLSC_CONFIG_COMPONENT, null)) {
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
				project_team_area: `${p_server}/rm/process/project-areas/${si_project}/team-areas/`,
				project_component: `${p_server}/rm/cm/component/${si_project}/`,
			});
		}
	}

	return {
		title: s_name_project,
		id: si_project,
		prefixes: h_prefixes_out,
		components: as_components,
		project: kd_project,
	};
}

export async function dng_export(gc_export) {
	const p_server = (new URL(gc_export.server)).origin;
	const ds_out = gc_export.output;

	const H_ENV = process.env;
	if(!H_ENV.DNG_USER || !H_ENV.DNG_PASS) {
		throw new Error(`Missing one of or both required environment variables: 'DNG_USER', 'DNG_PASS`);
	}

	const n_socket_limit = gc_export.sockets || 64;
	const n_concurrent_requests = gc_export.requests || n_socket_limit;

	const y_client = new SimpleOslcClient({
		server: p_server,
		username: H_ENV.DNG_USER,
		password: H_ENV.DNG_PASS,
		sockets: n_socket_limit,
		requests: n_concurrent_requests,
		context: gc_export.context,
		verbosity: gc_export.verbosity || H_ENV.DNG_EXPORT_DEBUG,
	});


	// authenticate
	await y_client.authenticate();

	// project info
	const {
		id: si_project,
		prefixes: h_prefixes,
		project: kd_project,
	}= await dng_project_info({
		...gc_export,
		prefixes: y_client._h_prefixes,
	});

	let p_query_project;
	let p_query_folder;
	let ds_scribe;
	SELECT_PROJECT: {
		// for writing rdf to stdout
		ds_scribe = new TurtleWriter({
			prefixes: h_prefixes,
		});

		ds_scribe.pipe(ds_out);


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

	// gather requirements
	const as_requirements = new Set();
	GATHER_REQUIREMENTS: {
		if(gc_export.useFolders) {
			const as_visited = new Set();

			// recursively gather requirments using folder workaround
			await (async function recurse(p_folder, a_path=[]) {
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
					await recurse(p_subfolder, [...a_path, s_title]);
				}
			})(`${p_server}/rm/folders/${si_project}`);
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
	const y_crawler = new Crawler(y_client, n_concurrent_requests, ds_scribe);

	// prep tasks
	const a_tasks = [];

	// each requirement
	for(const p_requirement of as_requirements) {
		// spawn crawler on this requirement
		a_tasks.push(y_crawler.spawn(p_requirement, 2));
	}

	// begin tasks
	await Promise.all(a_tasks);

	// done
	console.warn(`done`);
}

export default dng_export;
