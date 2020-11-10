import {once} from 'events';
import DataFactory from '@graphy/core.data.factory';
import chalk from 'chalk';
const cherr = chalk.stderr;

import {
	ElementFactory,
} from '../model/mms-elements.mjs';

import H_PREFIXES from '../common/prefixes.mjs';

const {
	c1,
	namedNode,
} = DataFactory;


const KT_RDF_TYPE = c1('a');
const KT_RDF_SUBJECT = c1('rdf:subject', H_PREFIXES);

const c1v = sc1 => c1(sc1, H_PREFIXES).concise();

const SV1_FOAF_NICK = c1v('foaf:nick');
const SV1_DCT_TITLE = c1v('dct:title');
const SV1_RDF_PREDICATE = c1v('rdf:predicate');
const SV1_RDF_OBJECT = c1v('rdf:object');

const SV1_OSLC_INSTANCE_SHAPE = c1v('oslc:instanceShape');


import {
	XM_MULTIPLICITY_ZERO,
	XM_MULTIPLICITY_MANY,
	MdkTranslator,
} from '../class/mdk-translator.mjs';


const first = as => [...as][0];
const all = as => [...as];

const as_warnings = new Set();
function warn_once(s_warn) {
	if(as_warnings.has(s_warn)) return;
	as_warnings.add(s_warn);
	console.warn(s_warn);
}

export class MmsUmlJsonTranslator extends MdkTranslator {
	constructor(...a_args) {
		super(...a_args);

		// create element factory
		const k_factory = this._k_factory = new ElementFactory('root', this._p_origin);

		// write root project element
		const sj_root = k_factory.create_class('root_pm', this._s_project_label).dump()
			.map(g => JSON.stringify(g, null, '\t')).join(',\n');

		this._ds_out.write(/* syntax: json */ `{"elements":[\n${sj_root}`);
	}

	_uml_type_for(sc1_value_type, as_ranges, as_objects) {
		const a_objects = as_objects? all(as_objects): [];

		// direct literal
		switch(sc1_value_type) {
			case 'xsd:integer': {
				return [{
					type: 'integer',
					values: a_objects.map(sv1_object => +c1(sv1_object).value),
				}];
			}

			case 'xsd:double': {
				return [{
					type: 'real',
					values: a_objects.map(sv1_object => +c1(sv1_object).value),
				}];
			}

			case 'xsd:boolean': {
				return [{
					type: 'boolean',
					values: a_objects.map(sv1_object => /true/i.test(c1(sv1_object).value)),
				}];
			}

			case 'xsd:string': {
				return [{
					type: 'string',
					values: a_objects.map(sv1_object => c1(sv1_object).value),
				}];
			}

			case 'xsd:dateTime': {
				return [{
					type: 'string',
					values: a_objects.map((sv1_object) => {
						return (new Date(c1(sv1_object).value)).toISOString();
					}),
				}];
			}

			default: break;
		}

		const a_ranges = all(as_ranges).map(this._f_c1pc);

		// range includes requirement
		if(a_ranges.includes('oslc_rm:Requirement')) {
			const a_reqs = [];
			const a_links = [];

			// each object
			for(const sv1_object of a_objects) {
				const kt_object = c1(sv1_object);

				// assert object term type
				if(!kt_object.isNamedNode) throw new Error(`expected range type IRI but found '${kt_object}'`);

				// object URI
				const p_object = kt_object.value;

				// convert to element id
				try {
					a_reqs.push(this._k_factory.uri_to_element(p_object));
				}
				// not a requirement
				catch(e_uri) {
					a_links.push(kt_object.value);
				}
			}

			const a_return = [];

			if(a_reqs.length) {
				a_return.push({
					type: 'relation',
					values: a_reqs,
				});
			}

			if(a_links.length) {
				a_return.push({
					type: 'string',
					values: a_links,
				});
			}

			return a_return;
		}

		// exactly one range
		if(1 === a_ranges.length) {
			const sc1_range = a_ranges[0];

			// not custom ENUM
			if(!sc1_range.startsWith('dng_type:')) {
				switch(sc1_range) {
					// person
					case 'foaf:Person': {
						return [{
							type: 'string',
							values: a_objects
								.map((sv1_object) => {
									// user is described in dataset
									if(sv1_object in this._hv3_trips) {
										return first(this._hv3_trips[sv1_object][SV1_FOAF_NICK]);
									}
									// for whatever reason, the user was not available/download; infer username from URI
									else {
										return /\/([^/]+)$/.exec(sv1_object)[1];
									}
								}),
						}];
					}

					default: {
						break;
					}
				}
			}
		}

		// title-ize
		return [{
			type: 'string',
			values: a_objects.map((sv1_object) => {
				const kt_object = c1(sv1_object);
				if(kt_object.isNamedNode) {
					return this._resource_title(kt_object.value);
				}
				else if(kt_object.isLiteral) {
					return kt_object.value;
				}
				else {
					throw new Error(`cannot generate title for ${kt_object.termType}`);
				}
			}),
		}];
	}

	translate_artifacts(b_tolerant=this._b_tolerant) {
		const {
			_kd_project: kd_project,
			_h_prefixes: h_prefixes,
			_f_c1p: c1p,
			_hv3_trips: hv3_trips,
			_k_factory: k_factory,
			_ds_out: ds_out,
		} = this;

		// select all requirements
		const kd_requirements = kd_project.match(null, KT_RDF_TYPE, c1p('oslc_rm:Requirement', h_prefixes));

		// each requirement
		for(const kq_req of kd_requirements) {
			// ref requirement IRI
			const p_requirement = kq_req.subject.value;

			// translate it
			this.translate_artifact(p_requirement, b_tolerant);
		}
	}

	artifact_to_element_id(p_requirement) {
		return this._k_factory.uri_to_element(p_requirement);
	}

	translate_artifact(p_requirement, b_tolerant=this._b_tolerant) {
		const {
			_h_prefixes: h_prefixes,
			_f_c1p: c1p,
			_hv3_trips: hv3_trips,
			_k_factory: k_factory,
			_kd_project: kd_project,
		} = this;

		// clone probs tree so we can delete visited properties
		const hv2_probs = Object.assign({}, hv3_trips['>'+p_requirement]);

		// de-reify any reified statements of this subject
		const kd_reified = kd_project.match(null, KT_RDF_SUBJECT, namedNode(p_requirement));
		for(const kq_reified of kd_reified) {
			const hv2_reified = hv3_trips[kq_reified.subject.concise()];

			// convert object named node term to predicate v1 string
			const sv1_predicate = first(hv2_reified[SV1_RDF_PREDICATE]);

			// copy objects set from reified quads over to tmp (cloned) artifact probs tree
			hv2_probs[sv1_predicate] = hv2_probs[sv1_predicate]
				? new Set([...hv2_probs[sv1_predicate], ...hv2_reified[SV1_RDF_OBJECT]])
				: hv2_reified[SV1_RDF_OBJECT];
		}

		// instance shape
		const as_shapes = hv2_probs[SV1_OSLC_INSTANCE_SHAPE];
		{
			// no shapes
			if(!as_shapes) {
				if(b_tolerant) {
					// prep element ID
					const si_element_req = k_factory.uri_to_element(p_requirement);

					// prep requirement title
					const s_title_req = '(Phantom Artifact)';

					// make artifact instance
					const k_artifact = k_factory.create_class(si_element_req, s_title_req);

					// serialize requirement
					this._ds_out.write(',\n'+k_artifact.dump().map(w => JSON.stringify(w, null, '\t')).join(',\n'));

					// do not continue
					return;
				}
				else {
					throw new Error(`artifact <${p_requirement}> has no OSLC instance shapes`);
				}
			}

			// too many shapes
			if(as_shapes.size > 1) {
				throw new Error(`artifact <${p_requirement}> has multiple instance shapes: ${[...as_shapes].map(sv1 => '<'+sv1.slice(1)+'>').join(', ')}`);
			}
		}

		// resolve shape
		const sv1_shape = first(as_shapes);

		// fetch shape def
		const g_def_shape = this._h_schema_shapes[sv1_shape];
		{
			// shape not found
			if(!g_def_shape) {
				throw new Error(`artifact <${p_requirement}> refers to instance shape not found in source graph: <${sv1_shape.slice(1)}>`);
			}
		}

		// make artifact instance
		const k_artifact = (() => {
			// prep element ID
			const si_element_req = k_factory.uri_to_element(p_requirement);

			// prep requirement title
			const s_title_req = c1p(first(hv2_probs[SV1_DCT_TITLE] || [''])).value;

			// remove dct:title from property hash
			delete hv2_probs[SV1_DCT_TITLE];

			// make artifact instance
			return k_factory.create_class(si_element_req, s_title_req);
		})();

		// add IRI source
		k_artifact.add({
			type: 'string',
			values: [p_requirement],
		}, 'source', 'Source');

		// each property in shape def
		for(const sv1_property in g_def_shape) {
			const p_property = sv1_property.slice(1);
			const sc1_property = namedNode(sv1_property.slice(1)).concise(h_prefixes);

			// ref property def
			const {
				multiplicity: xc_multiplicity,
				value_type: sc1_value_type,
				ranges: as_ranges,
				title: s_label_prop,
			} = g_def_shape[sv1_property];

			// ref objects set
			const as_objects = hv2_probs[sv1_property];

			// can be many
			let b_expects_many = xc_multiplicity & XM_MULTIPLICITY_MANY;
			let b_allows_zero = xc_multiplicity & XM_MULTIPLICITY_ZERO;

			// prep UML type
			const a_types = this._uml_type_for(sc1_value_type, as_ranges, as_objects);
			const b_multitype = a_types.length > 1;

			// property is missing
			if(!as_objects) {
				// multiplicty allows zero; skip it
				if(b_allows_zero) continue;

				// expects many
				if(b_expects_many) {
					let i_which = 0;
					for(const g_uml of a_types) {
						k_artifact.add_array(g_uml, p_property+(b_multitype? '_'+(i_which++): ''), s_label_prop);
					}
				}
				// does not expect many; add null property
				else {
					k_artifact.add_null(p_property, s_label_prop);
				}
			}
			// expects many; add array
			else if(b_expects_many) {
				let i_which = 0;
				for(const g_uml of a_types) {
					k_artifact.add_array(g_uml, p_property+(b_multitype? '_'+(i_which++): ''), s_label_prop);
				}
			}
			// expects exactly one
			else {
				let i_which = 0;
				for(const g_uml of a_types) {
					k_artifact.add(g_uml, p_property+(b_multitype? '_'+(i_which++): ''), s_label_prop);
				}
			}

			// remove property from hash
			delete hv2_probs[sv1_property];
		}

		// each unvisited property
		for(const [sv1_property, as_objects] of Object.entries(hv2_probs)) {
			const p_property = sv1_property.slice(1);
			const sc1_property = namedNode(sv1_property.slice(1)).concise(h_prefixes);
			const a_objects = all(as_objects);

			// prep label
			const s_label_prop = this._resource_title(p_property, true);

			// create object term
			const kt_object_0 = c1(a_objects[0]);

			// named node
			if(kt_object_0.isNamedNode) {
				const p_object_0 = kt_object_0.value;

				// local
				if(k_factory.origin_matches(p_object_0)) {
					// artifact
					if((new URL(p_object_0)).pathname.startsWith('/rm/resources/')) {
						k_artifact.add_array({
							type: 'relation',
							values: a_objects.map(sv1 => k_factory.uri_to_element(sv1.slice(1))),
						}, p_property, s_label_prop);
						continue;
					}
					// skip non-RDF
					else if(/^\/rm\/(process|cm|accessControl)\//.test(p_object_0)) {
						console.warn(cherr.yellow(`skipping ${sc1_property}: <${p_object_0}>`));
						continue;
					}
				}

				// all other cases
				k_artifact.add_array({
					type: 'string',
					values: all(as_objects).map(sv1 => this._resource_title(sv1.slice(1), true)),
				}, p_property, s_label_prop);
			}
			// literal value
			else if(kt_object_0.isLiteral) {
				warn_once(`wrapping Literal for unmapped property ${sc1_property}${kt_object_0.isDatatyped? ' of type '+kt_object_0.datatype.concise(h_prefixes): ''} as an array of strings`);

				// convert generically
				k_artifact.add_array({
					type: 'string',
					values: a_objects.map(sv1 => c1(sv1).value),
				}, p_property, s_label_prop);
			}
			// other
			else {
				throw new Error(`unexpected term type '${kt_object_0.termType}' for unmapped property ${sc1_property}`);
			}
		}

		// serialize requirement
		this._ds_out.write(',\n'+k_artifact.dump().map(w => JSON.stringify(w, null, '\t')).join(',\n'));
	}

	async end() {
		this._ds_out.end(']}');
		await once(this._ds_out, 'finish');
	}
}
