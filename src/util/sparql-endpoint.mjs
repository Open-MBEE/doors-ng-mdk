import HttpClient from './http-client.mjs';

import {fetch} from './io.mjs';
import stream from 'stream';

import StreamJson from './stream-json.js';
const {
	JsonStreamValues,
	JsonStreamArray,
	JsonPick,
	JsonParser,
} = StreamJson;

const K_GLOBAL_CLIENT = new HttpClient({
	// proxy: process.env.SPARQL_PROXY,
});

class QueryResponse {
	constructor(y_res) {
		this._y_res = y_res;
	}

	async rows() {
		let a_rows = [];

		for await (let g_row of this) {
			a_rows.push(g_row);
		}

		return a_rows;
	}
}


function SparqlEndpoint$prefix_string(k_self) {
	if(k_self._s_cached_prefix_string) return k_self._s_cached_prefix_string;

	let s_out = '';
	for(let [si_prefix, p_prefix] of Object.entries(k_self._h_prefixes)) {
		s_out += `PREFIX ${si_prefix}: <${p_prefix}> \n`;
	}

	return (k_self._s_cached_prefix_string = s_out);
}

let as_open = new Set();

function normalize_action(z_action) {
	let s_query;
	let g_headers = {};

	// action argument is a string
	if('string' === typeof z_action) {
		// 'cast' to string
		s_query = z_action;
	}
	// action argument is object (and not null)
	else if(z_action && 'object' === typeof z_action) {
		// destructure
		({
			sparql: s_query,
			headers: g_headers={},
		} = z_action);
	}
	// not supported
	else {
		throw new TypeError('invalid argument type for query');
	}

	return {
		sparql: s_query,
		headers: g_headers,
	};
}

async function SparqlEndpoint$submit(k_self, g_request, b_any=false) {
	let y_reqres;

	debugger;

	try {
		// y_reqres = await k_self._k_client.stream(g_request);
		return await fetch(g_request.url, g_request);
	}
	catch(e_req) {
		debugger;
		console.error(e_req);
		throw e_req;
	}

	// return new StreamingQueryResponse(y_reqres, b_any);
}

export class SparqlEndpoint {
	constructor(gc_endpoint) {
		let {
			url: p_endpoint,
			prefixes: h_prefixes={},
			client: k_client=K_GLOBAL_CLIENT,
		} = gc_endpoint;

		this._p_url = p_endpoint.replace(/\/$/, '');
		this._h_prefixes = h_prefixes;
		this._k_client = k_client;
		this._as_open = as_open;
	}

	async query(z_query) {
		let {
			sparql: s_query,
			headers: g_headers,
		} = normalize_action(z_query);

		console.info(`> """\n${s_query.split(/\n/g).trim().join('\t\n')}\n"""`);

		return await SparqlEndpoint$submit(this, {
			method: 'POST',
			url: `${this._p_url}/${process.env.SPARQL_QUERY_PATH || 'sparql'}`,
			headers: {
				accept: 'application/sparql-results+json',
				...(g_headers || {}),
			},
			form: {
				query: SparqlEndpoint$prefix_string(this)+s_query,
			},
		});
	}

	async update(z_update) {
		let {
			sparql: s_query,
			headers: g_headers,
		} = normalize_action(z_update);

		console.info(`> """\n${s_query.split(/\n/g).trim().join('\t\n')}\n"""`);

		return await SparqlEndpoint$submit(this, {
			method: 'POST',
			url: `${this._p_url}/${process.env.SPARQL_UPDATE_PATH || 'sparql'}`,
			headers: {
				accept: 'application/sparql-results+json',
				...(g_headers || {}),
			},
			form: {
				update: SparqlEndpoint$prefix_string(this)+s_query,
			},
		}, true);
	}
}
