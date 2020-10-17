import TurtleReader from '@graphy/content.ttl.read';
import FastDataset from '@graphy/memory.dataset.fast';
import TrigWriter from '@graphy/content.trig.write';
import DataFactory from '@graphy/core.data.factory';

import {pipeline as _pipeline} from 'stream';
import {promisify} from 'util';
const pipeline = promisify(_pipeline);

import {
	MmsUmlJsonTranslator,
} from '../class/mms-uml-json-translator.mjs';

import H_PREFIXES from '../common/prefixes.mjs';

const {
	c1,
} = DataFactory;

const KT_RDF_TYPE = c1('a');
const KT_OSLC_PROP_DEF = c1('oslc:propertyDefinition', H_PREFIXES);
const KT_OSLC_RESOURCE_SHAPE = c1('oslc:ResourceShape', H_PREFIXES);

const c1v = sc1 => c1(sc1, H_PREFIXES).concise();

const SV1_RDF_TYPE = c1v('a');
const SV1_RDFS_LABEL = c1v('rdfs:label');
const SV1_FOAF_NAME = c1v('foaf:name');
const SV1_FOAF_NICK = c1v('foaf:nick');
const SV1_FOAF_PERSON = c1v('foaf:person');
const SV1_DCT_TITLE = c1v('dct:title');
const SV1_OSLC_NAME = c1v('oslc:name');
const SV1_OSLC_RANGE = c1v('oslc:range');
const SV1_OSLC_PROPERTY = c1v('oslc:property');
const SV1_OSLC_PROPERTY_DEFINITION = c1v('oslc:propertyDefinition');
const SV1_OSLC_REQUIREMENT = c1v('oslc_rm:Requirement');

const SV1_OSLC_INSTANCE_SHAPE = c1v('oslc:instanceShape');


export async function dng_delta(gc_delta) {
	let kd_cached = FastDataset();
	let kd_exported = FastDataset();
	let h_prefixes = {};

	console.time('load');

	await Promise.all([
		pipeline(...[
			gc_delta.cached,
			new TurtleReader({
				data(g_quad) {
					if(g_quad.subject.isNamedNode) {
						kd_cached.add(g_quad);
					}
				},

				eof(_h_prefixes) {
					h_prefixes = {
						..._h_prefixes,
						...H_PREFIXES,
					};
				},
			}),
		]),

		pipeline(...[
			gc_delta.exported,
			new TurtleReader(),
			kd_exported,
		]),
	]);

	console.timeEnd('load');
	console.warn(`ready; ${kd_cached.size} quads in cached; ${kd_exported.size} quads in exported`);

	// create translator
	const k_translator = new MmsUmlJsonTranslator({
		server: gc_delta.server,
		project: gc_delta.project,
		prefixes: h_prefixes,
		dataset: kd_exported,
		output: gc_delta.adds,
	});

	console.time('delta');

	const kd_deleted = kd_cached.minus(kd_exported);

	const kd_added = kd_exported.minus(kd_cached);

	console.timeEnd('delta');

	console.warn(`ready; ${kd_deleted.size} quads deleted; ${kd_added.size} quads added`);

	// ref exported triples
	const hv3_trips_exported = kd_exported._h_quad_tree['*'];

	// deleted list
	const a_deleted = [];
	{
		// prep output stream for DELETE elements
		const ds_delete = gc_delta.deletes;
		ds_delete.write(/* syntax: json */ `{"elements":[{"id":"__NO_OP__"}\n`);

		// each distinct subject in deleted
		const hv3_trips = kd_deleted._h_quad_tree['*'];
		for(const sv1_subject in hv3_trips) {
			// skip blank nodes
			if('_' === sv1_subject[0]) continue;

			// ref probs
			const hv2_probs = hv3_trips_exported[sv1_subject];

			// requirement
			if(hv2_probs[SV1_RDF_TYPE]?.has(SV1_OSLC_REQUIREMENT)) {
				// add to 'deleted' list
				a_deleted.push(sv1_subject);

				const p_artifact = sv1_subject.slice(1);

				// write to output
				ds_delete.write(/* syntax: json */ `,\n{"id":"${k_translator.artifact_to_element_id(p_artifact)}"}`);
			}
		}

		// close delete stream
		ds_delete.write('\n]}');
	}


	// added list
	const a_added = [];
	{
		// each distinct subject in added
		const hv3_trips_added = kd_added._h_quad_tree['*'];
		for(const sv1_subject in hv3_trips_added) {
			// skip blank nodes
			if('_' === sv1_subject[0]) continue;

			// ref probs
			const hv2_probs = hv3_trips_exported[sv1_subject];

			// requirement
			if(hv2_probs[SV1_RDF_TYPE]?.has(SV1_OSLC_REQUIREMENT)) {
				// add to 'added' list
				a_added.push(sv1_subject);

				// translate artifact
				k_translator.translate_artifact(sv1_subject.slice(1));
			}
		}
	}

	// close output
	k_translator.end();

	return {
		added: a_added,
		deleted: a_deleted,
	};
}

export default dng_delta;
