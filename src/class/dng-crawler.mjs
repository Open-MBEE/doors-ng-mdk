import {URL} from 'url';
import FastDataset from '@graphy/memory.dataset.fast';
import factory from '@graphy/core.data.factory';
import pino from 'pino';
import chalk from 'chalk';

const cherr = chalk.stderr;
const logger = pino(pino.destination({dest:2, sync:false}));

import {
	decontextualize_quad,
} from '../class/oslc-client.mjs';
import AsyncLockPool from '../util/async-lock-pool.mjs';

import H_PREFIXES from '../common/prefixes.mjs';
import {
	SkipError,
	HttpError,
} from '../common/errors.mjs';

const A_NETWORK_RETRY_ERROR_CODES = [
	'ECONNRESET',
	'EPIPE',
	'ETIMEDOUT',
	'ENOTFOUND',
];

const P_OSLC_INSTANCE_SHAPE = factory.c1('oslc:instanceShape', H_PREFIXES).value;

const all = as => as? [...as]: [];
const first = as => all(as)[0];

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


export class DngCrawler {
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

		// prep decontextualizer if needed
		this._f_decontextualize_quad = y_client._p_context
			? decontextualize_quad
			: kt => kt;
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
		const f_decontextualize_quad = this._f_decontextualize_quad;

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
			k_dataset.add(f_decontextualize_quad(kt_quad));
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
