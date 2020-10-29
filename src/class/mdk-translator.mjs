import {URL} from 'url';

import DataFactory from '@graphy/core.data.factory';
import chalk from 'chalk';
const cherr = chalk.stderr;

import H_PREFIXES from '../common/prefixes.mjs';

const {
	c1,
	namedNode,
} = DataFactory;


const KT_RDF_TYPE = c1('a');
const KT_OSLC_RANGE = c1('oslc:range', H_PREFIXES);
const KT_OSLC_PROP_DEF = c1('oslc:propertyDefinition', H_PREFIXES);
const KT_OSLC_RESOURCE_SHAPE = c1('oslc:ResourceShape', H_PREFIXES);

const c1v = sc1 => c1(sc1, H_PREFIXES).concise();

const SV1_RDF_TYPE = c1v('a');
const SV1_RDFS_LABEL = c1v('rdfs:label');
const SV1_FOAF_NAME = c1v('foaf:name');
const SV1_FOAF_PERSON = c1v('foaf:person');
const SV1_DCT_TITLE = c1v('dct:title');
const SV1_OSLC_NAME = c1v('oslc:name');
const SV1_OSLC_RANGE = c1v('oslc:range');
const SV1_OSLC_VALUE_TYE = c1v('oslc:valueType');
const SV1_OSLC_PROPERTY = c1v('oslc:property');
const SV1_OSLC_PROPERTY_DEFINITION = c1v('oslc:propertyDefinition');
const SV1_JAZZ_PROJECT_AREA = c1v('jazz_proc:projectArea');


/* eslint-disable no-multi-spaces */
export const XM_MULTIPLICITY_ZERO = 0b001;
export const XM_MULTIPLICITY_ONE  = 0b010;
export const XM_MULTIPLICITY_MANY = 0b100;
export const XC_MULTIPLICITY_EXISTENTIAL = XM_MULTIPLICITY_ZERO | XM_MULTIPLICITY_ONE;
export const XC_MULTIPLICITY_EXACTLY_ONE = XM_MULTIPLICITY_ONE;
export const XC_MULTIPLICITY_STAR        = XM_MULTIPLICITY_ZERO | XM_MULTIPLICITY_MANY;
export const XC_MULTIPLICITY_PLUS        = XM_MULTIPLICITY_ONE  | XM_MULTIPLICITY_MANY;
/* eslint-enable no-multi-spaces */



const first = as => Array.from(as)[0];
const all = as => Array.from(as);

const decamelify = s_input => s_input.replace(/^\w/, s => s.toUpperCase())
	.replace(/([a-z])([A-Z][a-z]|[B-Z])/g, (s_, s_1, s_2) => s_1+' '+s_2[0].toUpperCase()+s_2.slice(1));


function local_part(p_thing) {
	// extract local part from URL
	return decamelify((new URL(p_thing)).pathname.replace(/^(.*?)([^#/]*)$/, '$2'));
}


const H_SCHEMA_DEFAULTS = {
	[SV1_RDF_TYPE]: {
		multiplicity: XC_MULTIPLICITY_STAR,
		value_type: null,
		ranges: new Set(),
		title: 'Type',
	},
};

export class MdkTranslator {
	constructor(gc_translator) {
		({
			dng_prefixes: this._h_prefixes,
			dng_project_name: this._s_project_label,
			mem_dataset: this._kd_project,
			local_output: this._ds_out,
		} = gc_translator);

		// save origin
		this._p_origin = (new URL(gc_translator.server)).origin;

		// tolerant
		this._b_tolerant = gc_translator.tolerant || false;

		// save trips tree
		this._hv3_trips = this._kd_project._h_quad_tree['*'];

		// c1 prefixed helper
		this._f_c1p = sc1 => c1(sc1, this._h_prefixes);
		this._f_c1pv = sc1 => c1(sc1, this._h_prefixes).concise();

		// load properties
		this._load_properties();

		// load shapes
		this._load_shapes();
	}

	/**
	* Creates an internal representation of the property schema definition
	* @returns {void}
	*/
	_load_properties() {
		const {
			_kd_project: kd_project,
			_h_prefixes: h_prefixes,
			_f_c1p: c1p,
			_hv3_trips: hv3_trips,
		} = this;

		// prepare project schema
		const h_schema_props = this._h_schema_props = {};

		// fetch all things that have property definition
		const kd_props = kd_project.match(null, KT_OSLC_PROP_DEF, null);

		// each property
		for(const kq_prop of kd_props) {
			// pull up property probs
			const sv1_node = kq_prop.subject.concise();
			const sv1_prop = kq_prop.object.concise();
			const h_probs = hv3_trips[sv1_node];

			// prep schema def
			const g_def = {
				multiplicity: XC_MULTIPLICITY_EXISTENTIAL,
			};

			// multiplicity
			const as_occurs = h_probs[c1p('oslc:occurs').concise()];
			if(as_occurs && as_occurs.size) {
				const sc1_occurs = c1(first(as_occurs)).concise(h_prefixes);

				switch(sc1_occurs) {
					case 'oslc:Zero-or-one': {
						g_def.multiplicity = XC_MULTIPLICITY_EXISTENTIAL;
						break;
					}

					case 'oslc:Exactly-one': {
						g_def.multiplicity = XC_MULTIPLICITY_EXACTLY_ONE;
						break;
					}

					case 'oslc:Zero-or-many': {
						g_def.multiplicity = XC_MULTIPLICITY_STAR;
						break;
					}

					case 'oslc:One-or-many': {
						g_def.multiplicity = XC_MULTIPLICITY_PLUS;
						break;
					}

					default: {
						throw new Error(`unhandlded property multiplicity ${sc1_occurs}`);
					}
				}
			}

			// override project area multiplicity
			if(SV1_JAZZ_PROJECT_AREA === sv1_prop) {
				g_def.multiplicity = XC_MULTIPLICITY_EXISTENTIAL;
			}

			// set ranges
			g_def.ranges = h_probs[SV1_OSLC_RANGE] || new Set();

			// set title string
			{
				// attempt to use dct:title or rdfs:label
				let s_title = c1(
					first(
						h_probs[SV1_DCT_TITLE]
						|| h_probs[SV1_RDFS_LABEL]
						|| new Set('"')  // eslint-disable-line comma-dangle
					)).value;

				// neither available, fallback to oslc:name but decamelify
				if(!s_title && h_probs[SV1_OSLC_NAME]) {
					s_title = decamelify(c1(first(h_probs[SV1_OSLC_NAME])).value);
				}

				// set title
				g_def.title = s_title;
			}

			// set value type
			const as_value_types = h_probs[SV1_OSLC_VALUE_TYE];
			g_def.value_type = as_value_types? c1p(first(as_value_types)): null;

			// save to schema
			h_schema_props[sv1_prop] = g_def;
		}
	}

	/**
	* Creates an internal representation of the shape schema definition
	* @returns {void}
	*/
	_load_shapes() {
		const {
			_kd_project: kd_project,
			_h_prefixes: h_prefixes,
			_f_c1p: c1p,
			_f_c1pv: c1pv,
			_hv3_trips: hv3_trips,
			_h_schema_props: h_schema_props,
		} = this;

		// prepare project schema
		const h_schema_shapes = this._h_schema_shapes = {};

		// fetch all resource shapes
		const kd_shapes = kd_project.match(null, KT_RDF_TYPE, KT_OSLC_RESOURCE_SHAPE);

		// each shape
		for(const kq_shape of kd_shapes) {
			// pull up shape probs
			const sv1_shape = kq_shape.subject.concise();
			const hv2_probs = hv3_trips[sv1_shape];

			// ref shape properties
			const h_properties = h_schema_shapes[sv1_shape] = {
				...H_SCHEMA_DEFAULTS,
			};

			// each property
			for(const sv1_prop of hv2_probs[SV1_OSLC_PROPERTY]) {
				// access properties
				const hv2_probs_props = hv3_trips[sv1_prop];

				// property definition resource
				const sv1_def_prop = first(hv2_probs_props[SV1_OSLC_PROPERTY_DEFINITION]);

				// pull up property def
				const g_def_prop = h_schema_props[sv1_def_prop];

				// no such def
				if(!g_def_prop) {
					throw new Error(`resource shape <${sv1_shape.slice(1)}> references property that is defined outside of local vocab: <${sv1_prop.slice(1)}>`);
				}

				// save to shape schema
				h_properties[sv1_def_prop] = g_def_prop;
			}
		}
	}

	/**
	* Produce the title of a given resource
	* @param {string} p_thing - IRI of the resource
	* @param {boolean} b_curie - whether or not to fallback to using CURIE
	* @returns {string} - resource title
	*/
	_resource_title(p_thing, b_curie=false) {
		const {
			_h_prefixes: h_prefixes,
			_k_factory: k_factory,
			_f_c1p: c1p,
		} = this;

		let d_thing;
		try {
			d_thing = new URL(p_thing);
		}
		catch(e_parse) {
			return null;
		}

		// remote IRI
		if(this._p_origin !== d_thing.origin) {
			// attempt to compress
			const sc1_thing = namedNode(p_thing).concise(h_prefixes);
			if('>' !== sc1_thing[0]) return sc1_thing;

			// use local part of IRI as title
			return local_part(p_thing);
		}

		// ref probs tree
		const hv2_probs = this._hv3_trips['>'+p_thing];

		// no info about this thing
		if(!hv2_probs) return null;

		// ref types
		const a_types = hv2_probs[SV1_RDF_TYPE];

		// is typed
		if(a_types && a_types.length) {
			// is person
			if(a_types.includes(SV1_FOAF_PERSON) && SV1_FOAF_NAME in hv2_probs) {
				// use name
				return c1p(first(hv2_probs[SV1_FOAF_NAME])).value;
			}
		}

		// attempt to find title
		const as_titles = hv2_probs[SV1_DCT_TITLE];
		if(as_titles) return c1p(first(as_titles)).value;

		// attemp to find label
		const as_labels = hv2_probs[SV1_RDFS_LABEL];
		if(as_labels) return c1p(first(as_labels)).value;

		// nothing
		return b_curie
			? namedNode(p_thing).concise(h_prefixes)
			: null;
	}
}
