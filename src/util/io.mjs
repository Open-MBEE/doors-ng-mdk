/* eslint-disable no-console, quote-props */
import fs from 'fs';
import https from 'https';
import stream from 'stream';

import util from 'util';
import chalk from 'chalk';

const cherr = chalk.stderr;

import StreamJson from '../util/stream-json.js';
const {
	JsonParser,
	JsonStreamValues,
	JsonStreamObject,
} = StreamJson;

// test if file exists
export function file_exists(p_file) {
	// attempt to access file
	try {
		fs.accessSync(p_file, fs.constants.R_OK);
	}
	catch(e_access) {
		return false;
	}

	return true;
}

// simple direct https request
export const request = (p_url, gc_request) => new Promise((fk_resolve) => {
	(https.request(p_url, gc_request, fk_resolve)).end();
});

// fetch remote JSON
export function fetch(p_url, gc_request, f_connected=null) {
	let ds_req;

	const dp_exec = new Promise((fk_resolve, fe_reject) => {
		// verbose
		if(process.env.DNG_MDK_DEBUG) {
			console.warn(cherr.blue(`HTTP ${gc_request.method || 'GET'} <${p_url}> config:`));
			const h_req_print = {...gc_request};
			delete h_req_print.agent;
			console.warn(util.inspect(h_req_print, false, 1, true));
		}

		ds_req = https.request(p_url, {
			...gc_request,
			headers: {
				...gc_request.headers,
				'Accept': 'application/json',
			},
		}, async(ds_res) => {
			// verbose
			if(process.env.DNG_MDK_DEBUG) {
				console.warn(cherr.yellow(`Received ${ds_res.statusCode} from endpoint w/ response headers:`));
				console.warn('\t'+cherr.grey(JSON.stringify(ds_res.headers)));
			}

			if(f_connected) {
				[fk_resolve, fe_reject] = f_connected();
			}

			const n_status = ds_res.statusCode;

			// good
			if(n_status >= 200 && n_status < 300) {
				let g_json = {};

				// load response body
				const ds_pipe = stream.pipeline([
					ds_res,
					JsonStreamObject.withParser(),
				], (e_pipe) => {
						if(e_pipe) {
							throw new Error(`Error while streaming parsing response JSON from <${p_url}>: ${e_pipe.stack}`);
						}
						else {
							fk_resolve(g_json);
						}
					});

				// response json object
				ds_pipe.on('data', ({key:si_key, value:w_value}) => {
					g_json[si_key] = w_value;
				});
			}
			// bad
			else {
				// download response body
				let s_body = '';
				for await(const s_chunk of ds_res) {
					s_body += s_chunk;
				}

				return fe_reject(new Error(`Unexpected response status ${n_status} from <${p_url}> '${ds_res.statusMessage}'; response body: '''\n${s_body}\n'''. Request metadata: ${JSON.stringify(gc_request, null, '\t')}`));
			}
		});
	});

	if(f_connected) {
		return ds_req;
	}
	else {
		ds_req.end();
		return dp_exec;
	}
}

const plain_object = z => 'object' === typeof z && null !== z && Object === z.constructor
	&& '[object Object]' === Object.prototype.toString.call(z);

// upload payload
export const upload = (z_input, p_url, gc_request) => new Promise((fk_resolve, fe_reject) => {
	let dt_waiting;

	// open request
	const ds_upload = fetch(p_url, gc_request, () => {
		clearInterval(dt_waiting);

		return [fk_resolve, fe_reject];
	});

	// string, Buffer, or ArrayBuffer; submit payload
	if('string' === typeof z_input || z_input?.byteLength) {
		ds_upload.end(z_input);
	}
	// stream
	else if('function' === typeof z_input.setEncoding) {
		return stream.pipeline([
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
	// json
	else if(plain_object(z_input)) {
		ds_upload.end(JSON.stringify(z_input));
	}
	// other
	else {
		throw new Error(`Not able to duck-type payload: ${z_input}`);
	}

	ds_upload.on('error', fe_reject);
	ds_upload.on('finish', () => {
		console.warn(`Payload successfully uploaded to <${p_url}>`);
		const t_start = Date.now();
		dt_waiting = setInterval(() => {
			const xs_elapsed = Math.round((Date.now() - t_start) / 1000);
			console.warn(`${Math.floor(xs_elapsed / 60)}m${((xs_elapsed % 60)+'').padStart(2, '0')}s have elapsed and still waiting...`);
		}, 1000*60*5);  // every 5 minutes
	});
});
