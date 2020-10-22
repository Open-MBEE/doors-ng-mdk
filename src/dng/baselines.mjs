import fs from 'fs';
import path from 'path';

import chalk from 'chalk';
const cherr = chalk.stderr;

import DataFactory from '@graphy/core.data.factory';
import SimpleOslcClient from '../class/oslc-client.mjs';
import AsyncLockPool from '../util/async-lock-pool.mjs';
import dng_folder from './folder.mjs';
import H_PREFIXES from '../common/prefixes.mjs';
import {
	SkipError,
	HttpError,
} from '../common/errors.mjs';

import {
	dng_export,
	dng_project_info,
} from './export.mjs';

const {
	c1,
} = DataFactory;

const all = as => as? [...as]: [];
const first = as => all(as)[0];

const c1v = sc1 => c1(sc1, H_PREFIXES).concise();

const SV1_RDF_TYPE = c1v('a');
const SV1_RDFS_MEMBER = c1v('rdfs:member');
const SV1_DCT_TITLE = c1v('dct:title');
const SV1_DCT_CREATED = c1v('dct:created');
const SV1_DCT_CREATOR = c1v('dct:creator');
const SV1_DCT_IDENTIFIER = c1v('dct:identifier');
const SV1_OSLC_CONFIG_OVERRIDES = c1v('oslc_config:overrides');
const SV1_OSLC_CONFIG_PREVIOUS_BASELINE = c1v('oslc_config:previousBaseline');
const SV1_OSLC_CONFIG_BASELINE_OF_STREAM = c1v('oslc_config:baselineOfStream');
const SV1_OSLC_CONFIG_STREAMS = c1v('oslc_config:streams');

const SV1_OSLC_CONFIG_BASELINE = c1v('oslc_config:Baseline');
const SV1_OSLC_CONFIG_STREAM = c1v('oslc_config:Stream');

const KT_OSLC_CONFIG_CONFIGURATIONS = c1('oslc_config:configurations', H_PREFIXES);

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

	const p_stream = g_root.bos;

	if(!g_root) {
		console.warn(`No inherent root baseline found for stream <${p_stream}>`);
		return Object.values(h_baselines)
			.sort((g_a, g_b) => (new Date(g_a)).created - (new Date(g_b)).created);
	}

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


export async function dng_update_baselines(gc_update) {
	// simple client
	const k_client = new SimpleOslcClient();

	// authenticate
	await k_client.authenticate();

	// deconstruct args
	const {
		components: as_components,
		project_id: si_project,
	} = gc_update;

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

	// load metadata for all baselines and streams
	for(const sv1_config of kd_configs._h_quad_tree['*']['>'+p_configs][SV1_RDFS_MEMBER]) {
		const p_config = sv1_config.slice(1);

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
			continue;
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
				description: firstv(SV1_DCT_CREATOR),
				bos: firstv(SV1_OSLC_CONFIG_BASELINE_OF_STREAM),
			};

			console.warn(cherr.blue(`<${p_config}> Baseline: ${s_title_config}`));
			c_baselines += 1;
		}
		// streams
		else if(as_types.has(SV1_OSLC_CONFIG_STREAM)) {
			h_streams[p_config] = {
				id: firstv(SV1_DCT_IDENTIFIER),
				uri: p_config,
				title: s_title_config,
				created: firstv(SV1_DCT_CREATED),
				creator: firstv(SV1_DCT_CREATOR),
				description: firstv(SV1_DCT_CREATOR),
			};
			console.warn(cherr.green(`<${p_config}> Stream: ${s_title_config}`));
			c_streams += 1;
		}
	}
	console.warn(`${c_baselines} baselines; ${c_streams} streams; ${c_streams_deleted} deleted streams`);

	// produce history for default stream
	const h_histories = stream_baseline_histories(h_baselines, h_streams);

	// save to project dir
	fs.writeFileSync(path.join(gc_update.project_dir, 'baselines.json'), JSON.stringify({
		histories: h_histories,
		map: h_baselines,
	}, null, '\t'));
}


export async function dng_export_baselines(gc_export) {
	const {
		project_id: si_project,
		project_dir: pd_project,
	} = gc_export;

	const {
		histories: h_histories,
		map: h_baselines,
	} = JSON.parse(fs.readFileSync(path.join(pd_project, 'baselines.json'), 'utf8'));

	const a_history = Object.values(h_histories)[0];

	// download each baseline successively
	for(const p_baseline of a_history) {
		const g_baseline = h_baselines[p_baseline];

		await dng_export({
			...gc_export,
			context: p_baseline,
			output: fs.createWriteStream(path.join(pd_project, 'baselines', g_baseline.id)),
		});
	}
}
