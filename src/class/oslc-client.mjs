import https from 'https';
import url from 'url';
import querystring from 'querystring';
import util from 'util';

import {RdfXmlParser} from 'rdfxml-streaming-parser';
import {JsonLdParser} from 'jsonld-streaming-parser';
import factory from '@graphy/core.data.factory';
import FastDataset from '@graphy/memory.dataset.fast';
import CookieManager from 'cookie-manager';
import chalk from 'chalk';
const cherr = chalk.stderr;

import H_PREFIXES from '../common/prefixes.mjs';
import {
	SkipError,
	HttpError,
} from '../common/errors.mjs';

const {
	c1,
} = factory;

const KT_OSLC_RM_SERVICE_PROVIDERS = c1('oslc_rm_1:rmServiceProviders', H_PREFIXES);

/**
* Make an HTTPS request
* @param {SimpleOslcClient} k_self - self instance object
* @param {string} s_url - relative pathname or absolute URL to request
* @param {hash} h_args - additional args to send to request call
* @param {any} w_body - contents to write to request payload
* @returns {Promise<http.ClientRequest>} - the request stream
*/
function SimpleOslcClient$request(k_self, s_url, h_args={}, w_body='') {
	// async
	return new Promise((fk_resolve, fe_reject) => {
		// create URL (from relative if necessary)
		const d_url = new url.URL(s_url, k_self._p_server);
		const p_url = d_url.toString();

		// create request descriptor
		const h_req = {
			method: 'GET',
			...h_args,
			headers: {
				accept: 'text/html',
				'oslc-core-version': '2.0',
				...h_args.headers,
				cookie: k_self._y_cookies.prepare(p_url),
			},
			agent: k_self._d_agent,
		};

		// verbose
		if(k_self._b_verbose) {
			console.warn(cherr.blue(`HTTP ${h_args.method || 'GET'} ${d_url.pathname}${d_url.search}; config:`));
			const h_req_print = {...h_req};
			delete h_req_print.agent;
			console.warn(util.inspect(h_req_print, false, 1, true));
		}

		// submit fetch resource request
		const ds_req = https.request(p_url, h_req, (ds_res) => {
			// verbose
			if(k_self._b_verbose) {
				console.warn(cherr.yellow(`Received ${ds_res.statusCode} from endpoint w/ response headers:`));
				console.warn('\t'+cherr.grey(JSON.stringify(ds_res.headers)));
			}

			// set custom property
			ds_res.req.originalUrl = d_url;

			// wants us to update cookie; do it
			const a_cookie_set = ds_res.headers['set-cookie'];
			if(a_cookie_set) k_self._y_cookies.store(p_url, a_cookie_set);

			fk_resolve(ds_res);
		}).on('error', fe_reject);

		// close request
		ds_req.end(w_body);
	});
}

/**
* Make an HTTPS request, and follow any redirects that arise
* @param {SimpleOslcClient} k_self - self instance object
* @param {string} s_url - relative pathname or absolute URL to request
* @param {hash} w_args - additional args to send to request call
* @param {any} w_body - contents to write to request payload
* @param {number} c_redirects - the number of redirects that have occurred
* @returns {Promise<http.ClientRequest>} - the request stream
*/
async function SimpleOslcClient$follow(k_self, s_url, w_args={}, w_body='', c_redirects=0) {
	// submit request
	const ds_res = await SimpleOslcClient$request(k_self, s_url, {
		...w_args,
		headers: {
			...(w_args.headers || {}),
		},
	}, w_body);

	// ref response status code
	let n_status = ds_res.statusCode;

	// follow redirect
	if((n_status >= 300 && n_status<=399)) {
		// max redirects
		if(c_redirects > 10) {
			throw new Error(`exceeded maximum redirects`);
		}

		// close socket
		ds_res.destroy();

		// ref redirect URL
		const p_redirect = ds_res.headers.location;

		// verbose
		if(k_self._b_verbose) {
			const d_req = ds_res.req;
			console.warn(`following redirect <${d_req.protocol}//${d_req.host}${d_req.path}> => <${p_redirect}>`);
		}

		// follow redirect (downgrade to simple GET requests)
		return await SimpleOslcClient$follow(k_self, p_redirect, {
			headers: {
				referer: ds_res.req.originalUrl.toString(),
			},
		}, '', c_redirects+1);
	}
	else {
		return ds_res;
	}
}

/**
* A simple OSLC client that handles authentication, and fetching RDF resources.
*/
export class SimpleOslcClient {
	constructor(gc_client) {
		const p_server = this._p_server = gc_client.server;
		this._s_username = gc_client.username;
		this._s_password = gc_client.password;
		this._y_cookies = new CookieManager();
		this._b_verbose = gc_client.verbose;
		this._d_agent = new https.Agent({
			keepAlive: true,
			maxSockets: gc_client.sockets,
			// scheduling: 'lifo',
			timeout: 12000,  // 12 seconds
		});

		// make prefix map
		this._h_prefixes = {
			...H_PREFIXES,
			dng_rm: `${p_server}/rm/`,
			dng_type: `${p_server}/rm/types/`,
			dng_resource: `${p_server}/rm/resources/`,
			dng_pa: `${p_server}/rm/process-authoring/`,
			dng_ppa: `${p_server}/rm/process/project-areas/`,
			dng_team_area: `${p_server}/rm/process/team-areas/`,
			dng_oslc: `${p_server}/rm/oslc_rm/`,
			dng_process: `${p_server}/rm/process/`,
			dng_folder: `${p_server}/rm/folders/`,
			dng_service: `${p_server}/rm/service/`,
			dng_acclist: `${p_server}/rm/acclist#`,
			dng_acccntrl: `${p_server}/rm/accessControl/`,
			dng_component: `${p_server}/rm/cm/component/`,
			dng_baseline: `${p_server}/rm/cm/baseline/`,
			dng_jts: `${p_server}/jts/`,
			dng_user: `${p_server}/jts/users/`,
		};
	}

	/**
	* Get the prefix map
	*/
	get prefixes() {
		return this._h_prefixes;
	}

	/**
	* Authenticate the client against by starting a session and obtaining the necessary cookie.
	* @returns {Promise} - resolves once the session has started
	*/
	async authenticate() {
		// start session
		await SimpleOslcClient$follow(this, `/rm/loginRedirect?redirect=${encodeURIComponent(this._p_server+'/rm')}`);

		// submit authentication request
		const ds_res_auth = await SimpleOslcClient$follow(this, '/jts/auth/j_security_check', {
			method: 'POST',
			headers: {
				'content-type': 'application/x-www-form-urlencoded',
			},
		}, querystring.stringify({
			j_username: this._s_username,
			j_password: this._s_password,
		}));

		// read body to end no matter what
		let s_body_auth = '';
		for await(const s_chunk of ds_res_auth) {
			s_body_auth += s_chunk;
		}

		// authentication failed
		if('authfailed' === ds_res_auth.headers['x-com-ibm-team-repository-web-auth-msg']) {
			throw new Error(`Authentication failed.\n${s_body_auth}`);
		}

		// success
		let n_status = ds_res_auth.statusCode;
		if((n_status >= 200 && n_status<=299)) {
			return s_body_auth;
		}
		// error
		else {
			throw new Error(`non-2xx HTTP response: ${n_status}\n${s_body_auth}`);
		}
	}

	/**
	* Fetch an RDF resource from the server
	* @param {string} pr_resource - relative pathname or absolute URL to resource
	* @returns {Promise<ReadableStream<RDFJS.Quad>>} - resolves once the
	*   resource has started downloading; readable stream of quad objects
	*/
	async fetch(pr_resource) {
		const ds_res = await SimpleOslcClient$follow(this, pr_resource, {
			headers: {
				accept: 'application/rdf+xml, application/ld+json',
			},
		});

		// ref headers
		const h_headers = ds_res.headers;
		const n_status = ds_res.statusCode;

		// 2xx response
		if(n_status >= 200 && n_status <= 299) {
			// ref content-type
			const s_content_type = h_headers['content-type'];
			if(s_content_type) {
				// RDF+XML
				if(s_content_type.startsWith('application/rdf+xml')) {
					// create parser
					const ds_parser = new RdfXmlParser({
						dataFactory: factory,
						baseIRI: ds_res.req.originalUrl.toString(),
					});

					// pipe response to parser
					ds_res.pipe(ds_parser);

					// pipe body to parser
					return ds_parser;
				}
				// JSON-LD
				else if(s_content_type.startsWith('application/ld+json')) {
					// create parser
					const ds_parser = new JsonLdParser({
						dataFactory: factory,
						baseIRI: ds_res.req.originalUrl.toString(),
					});

					// pipe response to parser
					ds_res.pipe(ds_parser);

					// pipe body to parser
					return ds_parser;
				}
				// image
				else if(s_content_type.startsWith('image/')) {
					// TODO: download it?

					// close underlying resources
					ds_res.destroy();

					// skip for now ;)
					throw new SkipError();
				}
				// html
				else if(s_content_type.startsWith('text/html')) {
					// read body to end
					let s_body = '';
					for await(const s_chunk of ds_res) {
						s_body += s_chunk;
					}

					// just log it
					console.warn(`${n_status} response from <${ds_res.req.originalUrl}>`);
					console.warn(cherr.cyan(s_body));

					// then skip
					throw new SkipError();
				}
				// non-text
				else {
					// close underlying resources
					ds_res.destroy();

					// just skip
					throw new SkipError();
				}
			}
		}

		// read body to end
		let s_body = '';
		for await(const s_chunk of ds_res) {
			s_body += s_chunk;
		}

		// close underlying resources
		ds_res.destroy();

		// throw error
		throw new HttpError({
			url: pr_resource,
			message: `Server returned ${ds_res.statusCode}`,
			status: ds_res.statusCode,
			headers: h_headers,
			body: s_body,
		});
	}

	/**
	* Fetch an RDF resource from the server, and load it into an in-memory dataset
	* @param {string} pr_resource - relative pathname or absolute URL to resource
	* @returns {Promise<ReadableStream<RDFJS.Quad>>} - resolves once the
	*   resource has been fully loaded into memory
	*/
	async load(pr_resource) {
		const ds_parser = await this.fetch(pr_resource);

		const k_dataset = FastDataset();
		for await(const kt_quad of ds_parser) {
			k_dataset.add(kt_quad);
		}

		return k_dataset;
	}

	/**
	* Fetch the root services of the requirements management
	* @yields {string} - the URL of each service provider
	*/
	async* root_services() {
		const ds_rdf = await this.fetch('/rm/rootservices');

		for await(let kt_quad of ds_rdf) {
			if(KT_OSLC_RM_SERVICE_PROVIDERS.equals(kt_quad.predicate)) {
				yield kt_quad.object.value;
			}
		}
	}
}

export default SimpleOslcClient;
