/* eslint-disable no-unused-expressions, no-console, quote-props */
import fs from 'fs';
import os from 'os';
import path from 'path';
import {URL, URLSearchParams} from 'url';
import https from 'https';
import stream from 'stream';
import {once} from 'events';

import chalk from 'chalk';
const cherr = chalk.stderr;

import {
	hash,
	request,
	fetch,
	upload,
} from '../util/io.mjs';

// ref env
const H_ENV = process.env;

// headers for json exchange
const H_HEADERS_JSON = {
	'Accept': 'application/json',
	'Content-Type': 'application/json',
};

function remove_meta(h_obj) {
	for(const si_key in h_obj) {
		if('_' === si_key[0] && '_appliedStereotypeIds' !== si_key) {
			delete h_obj[si_key];
		}

		const z_value = h_obj[si_key];
		if('object' === typeof z_value && !Array.isArray(z_value)) {
			remove_meta(z_value);
		}
	}

	return h_obj;
}

function canonicalize(z_a) {
	if(Array.isArray(z_a)) {
		// object nested
		const w_0 = z_a[0];
		if(w_0 && 'object' === typeof w_0) {
			// canonicalize items
			const a_items = z_a.map(w => canonicalize(w));

			if(Array.isArray(w_0)) {
				return a_items.sort();
			}
			else {
				return a_items.sort((g_a, g_b) => g_a.id < g_b.id);
			}
		}

		return z_a.sort();
	}
	else if('object' === typeof z_a && null !== z_a) {
		const h_out = {};
		const a_keys = Object.keys(z_a).sort();
		for(const si_key of a_keys) {
			h_out[si_key] = canonicalize(z_a[si_key]);
		}
		return h_out;
	}
	else {
		return z_a;
	}
}

function compute_delta(h_a, h_b) {
	const a_added = [];
	const a_deleted = [];

	// each key in a
	for(const si_key in h_a) {
		// key is also in b
		if(si_key in h_b) {
			// values differ; overwrite element
			if(JSON.stringify(canonicalize(h_a[si_key])) !== JSON.stringify(canonicalize(h_b[si_key]))) {
				a_added.push(h_b[si_key]);
			}

			// delete from b
			delete h_b[si_key];
		}
		// key is not in b; entry was deleted
		else {
			a_deleted.push(si_key);
		}
	}

	// each remaining key in b
	for(const si_key in h_b) {
		a_added.push(h_b[si_key]);
	}

	return {
		added: a_added,
		deleted: a_deleted,
	};
}

export class MmsProject {
	constructor(gc_mms) {
		// mms server
		let p_server_mms = H_ENV.MMS_SERVER;
		if(!p_server_mms) {
			throw new Error(`Must provide an MMS server URL via env var 'MMS_SERVER'`);
		}
		p_server_mms = (new URL(p_server_mms)).origin;

		// mms creds
		if(!H_ENV.MMS_USER || !H_ENV.MMS_PASS) {
			throw new Error(`Missing one of or both required environment variables: 'MMS_USER', 'MMS_PASS`);
		}

		this._p_server = p_server_mms;
		this._si_org = gc_mms.mms_project_org;
		this._si_project = gc_mms.mms_project_id;
		this._s_project_name = gc_mms.dng_project_name;
		this._b_safety = false;
		this._n_batch = 100e3;

		const h_headers = this._h_headers = {
			...H_HEADERS_JSON,
			'Authorization': `Basic ${Buffer.from(H_ENV.MMS_USER+':'+H_ENV.MMS_PASS).toString('base64')}`,
			'Transfer-Encoding': 'chunked',
		};

		this._p_endpoint_service = `${this._p_server}/alfresco/service`;
		this._p_endpoint_project = `${this._p_endpoint_service}/projects/${this._si_project}`;
		this._p_endpoint_refs = `${this._p_endpoint_project}/refs`;

		const gc_req = {
			headers: h_headers,
		};

		this._gc_req_get = {
			...gc_req,
			method: 'GET',
		};

		this._gc_req_put = {
			...gc_req,
			method: 'PUT',
		};

		this._gc_req_post = {
			...gc_req,
			method: 'POST',
		};

		this._gc_req_delete = {
			...gc_req,
			method: 'DELETE',
		};
	}

	_endpoint_elements(si_ref, h_query=null) {
		return `${this._p_endpoint_refs}/${si_ref}/elements${h_query? '?'+new URLSearchParams(h_query): ''}`;
	}

	_endpoint_element_ids(si_ref, h_query=null) {
		return `${this._p_endpoint_refs}/${si_ref}/elementIds${h_query? '?'+new URLSearchParams(h_query): ''}`;
	}


	/**
	* Fetch the version of MMS from the server
	* @returns {Promise<string>} - the semver string
	*/
	async mms_version() {
		return (await fetch(`${this._p_endpoint_service}/mmsversion`, this._gc_req_get)).mmsVersion;
	}


	/**
	* Ensure the project exists, creating it if necessary
	* @param {boolean} b_reset - whether or not to delete and recreate existing project
	* @returns {boolean} - true iff the project was (re)created
	*/
	async create(b_reset=false) {
		const ds_res = await request(this._p_endpoint_project, this._gc_req_get);
		let b_create = false;

		// project does not exist
		if(404 === ds_res.statusCode) {
			console.warn(`'${this._si_project}' does not yet exist on <${this._p_server}>; creating project...`);
			b_create = true;
		}
		// project exists; delete existing
		else if(b_reset) {
			console.log(`DELETE project from <${this._p_endpoint_project}>...`);
			console.time('reset');

			// submit request
			await request(this._p_endpoint_project, this._gc_req_delete);

			console.timeEnd('reset');
			b_create = true;
		}

		// create new project
		if(b_create) {
			console.time('create');

			// create project
			await upload(JSON.stringify({
				projects: [{
					type: 'Project',
					orgId: this._si_org,
					id: this._si_project,
					name: this._s_project_name.trim().replace(/\s+/g, ' '),
				}],
			}), `${this._p_endpoint_service}/orgs/${this._si_org}/projects`, this._gc_req_post);

			console.timeEnd('create');
		}

		return b_create;
	}


	/**
	* Fetch all refs for the project as a hash
	* @returns {RefHash} - all refs as {[ref_id]: ref}
	*/
	async refs() {
		// fetch current refs
		const g_body_refs = await fetch(this._p_endpoint_refs, this._gc_req_get);

		// coerce into map
		return g_body_refs.refs.reduce((h_out, g_ref) => ({
			...h_out,
			[g_ref.id]: g_ref,
		}), {});
	}


	/**
	* Compute the delta between two element hashes and commit them to HEAD
	* @param {ElementHash} h_elements_old - the old elements hash
	* @param {ElementHash} h_elements_new - the new elements hash
	* @param {string} si_ref - which ref to use
	* @returns {void}
	*/
	async apply_deltas(h_elements_old, h_elements_new, si_ref='master') {
		// do not compare meta elements
		delete h_elements_old[this._si_project];
		delete h_elements_new[this._si_project];

		// diff jsons
		const {
			added: a_added,
			deleted: a_deleted,
		} = compute_delta(h_elements_old, h_elements_new);

		// re-assign old now in case GC wants to free up mem
		h_elements_old = null;

		console.warn(`Applying ${a_deleted.length} deletions and ${a_added.length} additions to ${si_ref}.`);

		// deletions
		if(a_deleted.length) {
			const ds_delete = new stream.PassThrough();
			const dp_upload = upload(ds_delete, this._endpoint_elements(si_ref, {overwrite:true}), this._gc_req_delete);

			ds_delete.write(/* syntax: json */ `{"elements":[`);
			let i_element = 0;
			for(const si_element of a_deleted) {
				ds_delete.write((i_element++? ',': '')+/* syntax: json */ `\n{"id":"${si_element}"}`);
			}
			ds_delete.end(/* syntax: json */ `\n]}`);

			await once(ds_delete, 'finish');
			await dp_upload;
		}

		// additions
		if(a_added.length) {
			const ds_add = new stream.PassThrough();
			const dp_upload = upload(ds_add, this._endpoint_elements(si_ref, {overwrite:true}), this._gc_req_post);

			ds_add.write(/* syntax: json */ `{"elements":[`);
			let i_element = 0;
			for(const g_element of a_added) {
				ds_add.write((i_element++? ',': '')+'\n'+JSON.stringify(g_element));
			}
			ds_add.end(/* syntax: json */ `\n]}`);

			await once(ds_add, 'finish');
			await dp_upload;
		}

		// return summary (number of elements deleted/added)
		return {
			deleted: a_deleted.length,
			added: a_added.length,
		};
	}


	/**
	* POST a JSON file to the project
	* @param {path} pr_json - absolute/relative path to local JSON file
	* @param {string} si_ref - which ref to use
	* @returns {void}
	*/
	async upload_json_stream(ds_upload, si_ref='master') {
		return await upload(ds_upload, this._endpoint_elements(si_ref, {overwrite:true}), this._gc_req_post);
	}


	/**
	* Tag the current HEAD of the given ref as a basline
	* @param {Baseline} g_baseline - the baseline descriptor
	* @param {string} si_ref - which ref to use
	* @returns {void}
	*/
	async tag_head_as_baseline(g_baseline, si_ref='master') {
		// commit tag
		await upload(JSON.stringify({
			refs: [{
				id: hash(`baseline.${g_baseline.id}`),
				name: g_baseline.title,
				parentRefId: si_ref,
				type: 'Tag',
				uri: g_baseline.uri,
				created: g_baseline.created,
				creator: g_baseline.creator,
				overrides: g_baseline.overrides,
				previous: g_baseline.previous,
				streams: g_baseline.streams,
				description: g_baseline.description,
				basedOnStream: g_baseline.bos,
			}],
		}), this._p_endpoint_refs, this._gc_req_post);
	}

	/**
	* Enable safety mechanism when fetching all elements
	* @returns {void}
	*/
	enable_safety() {
		this._b_safety = true;
	}


	async _fetch_project(si_ref) {
		// safety enabled
		if(this._b_safety) {
			// fetch all element ids
			const g_preload = await fetch(this._endpoint_element_ids(si_ref), this._gc_req_get);

			// destructure
			const {
				commitId: si_commit,
				elements: a_elements,
			} = g_preload;

			// more than BATCH elements
			const n_batch = this._n_batch;
			const nl_elements = a_elements.length;
			if(nl_elements > n_batch) {
				// project elements
				let a_project = [];

				// approx 1.75 KiB per element, 2 GiB max payload
				// batch into 100k elements per request
				for(let i_slice=0; i_slice<nl_elements; i_slice+=n_batch) {
					// select batch
					const a_chunk = g_preload.elements.slice(i_slice, i_slice+n_batch);

					// get elements in batch
					const g_response = await upload({
						elements: a_chunk,
					}, this._endpoint_elements(si_ref), this._gc_req_put);

					// update concat
					a_project = a_project.concat(g_response.elements);
				}

				// reconstruct payload
				return {
					elements: a_project,
				};
			}
		}

		// get all elements
		await fetch(this._endpoint_elements(si_ref), this._gc_req_get);
	}

	/**
	* Load the entire contents of an MMS project into memory
	* @param {string} si_ref - which ref to use
	* @returns {Promise} - the elements hash as {[element_id]: element}
	*/
	async load(si_ref='master') {
		const si_project = this._si_project;
		const g_project = await this._fetch_project(si_ref);
		const a_elements = g_project.elements;

		// prep output hash
		const h_out = {};

		// each element
		for(const g_element of a_elements) {
			// skip project element
			if('Project' === g_element.type) continue;

			// project is owner
			if(si_project === g_element.ownerId && 'Package' === g_element.type) {
				// do not compare special elements
				if('Holding Bin' === g_element.name || 'View Instances Bin' === g_element.name) {
					continue;
				}
			}

			// otherwise, add to hash
			h_out[g_element.id] = remove_meta(g_element);
		}

		// return hash
		return h_out;
	}
}

