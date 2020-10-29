/* eslint-disable class-methods-use-this */
import crypto from 'crypto';
import {URL} from 'url';
import chalk from 'chalk';
const cherr = chalk.stderr;

const sha256_hex = s_input => crypto.createHash('sha256').update(s_input).digest('hex');

const first = as => [...as][0];
const all = as => [...as];

const H_TYPES = {
	string: '_9_0_2_91a0295_1110274713995_297054_0',
	integer: 'donce_1051693917650_319078_0',
	real: '_17_0beta_f720368_1291217394082_340077_1886',
	boolean: '_12_0EAPbeta_be00301_1157529792739_987548_11',
	null: null,
};

export class Thing {
	constructor(k_factory, si_self, si_owner) {
		this._k_factory = k_factory;
		this._si_self = si_self;
		this._si_owner = si_owner;
		this._h_export = this._init();
	}

	_init() {
		return {};
	}

	add(h_add) {
		this._h_export = {
			...this._h_export,
			...h_add,
		};
	}

	get type() {
		if(this._h_export.type) return this._h_export.type;

		throw new Error('overriding subclass does not implement #type');
	}

	get id() {
		return this._si_self;
	}

	get ownerId() {
		return this._si_owner;
	}

	get name() {
		if(this._h_export.name) return this._h_export.name;

		throw new Error('overriding subclass does not implement #name');
	}

	get visibility() {
		if(this._h_export.visibility) return this._h_export.visibility;

		throw new Error('overriding subclass does not implement #visibility');
	}

	extras() {
		return {};
	}

	export() {
		return {
			_appliedStereotypeIds: [],
			documentation: '',
			type: this.type,
			id: this.id,
			mdExtensionsIds: [],
			ownerId: this.ownerId,
			syncElementId: null,
			appliedStereotypeInstanceId: null,
			clientDependencyIds: [],
			supplierDependencyIds: [],
			name: this.name,
			nameExpression: null,
			visibility: this.visibility,
			templateParameterId: null,
			...this._h_export,
			...this.extras(),
		};
	}

	dump() {
		return [this.export()];
	}
}


export class Literal extends Thing {
	constructor(k_factory, k_owner, s_mod, h_add={}) {
		super(k_factory, k_owner.id+'_value'+(s_mod? '_'+s_mod: ''), k_owner.id);
		this.add(h_add);
	}

	get visibility() {
		return 'public';
	}

	get name() {
		return '';
	}

	export() {
		return {
			...super.export(),
			typeId: null,
		};
	}
}


export class Element extends Thing {
	_init() {
		return {
			...super._init(),
			isLeaf: false,
		};
	}

	get visibility() {
		return null;
	}
}


export class Attribute extends Element {
	constructor(k_factory, si_self, si_owner, s_name, si_type) {
		super(k_factory, si_self, si_owner);
		this._s_name = s_name;
		this._si_type = si_type;
	}

	_init() {
		return {
			...super._init(),
			isOrdered: false,
			isUnique: true,
			lowerValue: null,
			upperValue: null,
			isReadOnly: false,
			templateParameterId: null,
			endIds: [],
			deploymentIds: [],
			aggregation: 'none',
			associationEndId: null,
			qualifierIds: [],
			datatypeId: null,
			defaultValue: null,
			interfaceId: null,
			isDerived: false,
			isDerivedUnion: false,
			isID: false,
			redefinedPropertyIds: [],
			subsettedPropertyIds: [],
			associationId: null,
		};
	}

	get type() {
		return 'Property';
	}

	get name() {
		return this._s_name;
	}

	get visibility() {
		return 'private';
	}

	get typeId() {
		return this._si_type;
	}

	defaultValue(k_value) {
		this.add({
			defaultValue: k_value.export(),
		});
	}

	export() {
		return {
			...super.export(),
			typeId: this._si_type,
		};
	}
}


export class Class extends Element {
	constructor(k_factory, si_self, s_name) {
		super(k_factory, si_self, k_factory.projectId+'_pm');
		this._s_name = s_name;
		this._a_baggage = [];
	}

	_init() {
		return {
			...super._init(),
			elementImportIds: [],
			packageImportIds: [],
			templateBindingIds: [],
			useCaseIds: [],
			representationId: null,
			collaborationUseIds: [],
			generalizationIds: [],
			powertypeExtentIds: [],
			isAbstract: false,
			isFinalSpecialization: false,
			redefinedClassifierIds: [],
			substitutionIds: [],
			classifierBehaviorId: null,
			interfaceRealizationIds: [],
			ownedOperationIds: [],
			isActive: false,
		};
	}

	get type() {
		return 'Class';
	}

	get name() {
		return this._s_name;
	}

	export() {
		return {
			...super.export(),
			ownedAttributeIds: this._a_baggage.map(k => k.id),
		};
	}

	dump() {
		return [
			this.export(),
			...this._a_baggage.flatMap(k => k.dump()),
		];
	}

	attribute(s_key, s_label, si_type) {
		const si_key_base = this.id+'_'+s_key;
		const si_key_hashed = sha256_hex(si_key_base);
		const k_attr = new Attribute(this._k_factory, si_key_hashed, this.id, s_label, si_type);
		this._a_baggage.push(k_attr);
		return k_attr;
	}

	add_array({type:s_type, values:a_values_in}, s_key, s_label) {
		const k_attr = this.attribute(s_key, s_label, H_TYPES[s_type]);

		// prep expression container
		const a_values_out = [];
		const k_container = new Literal(this._k_factory, k_attr, '', {
			type: 'Expression',
			symbol: '',
			operand: a_values_out,
		});

		delete k_container._h_export.value;

		// check type
		if('relation' === s_type) {
			console.warn(cherr.yellow(`serializing experimental relation bag for ${s_key}`));

			for(let i_value=0, nl_values=a_values_in.length; i_value<nl_values; i_value++) {
				this.add_relation(s_key+'_'+i_value, s_label, a_values_in[i_value]);
			}

			return this;
		}

		// export values
		a_values_out.push([...a_values_in.map((w_value, i_value) => (
			new Literal(this._k_factory, k_container, i_value+'', {
				type: 'Literal'+s_type[0].toUpperCase()+s_type.slice(1),
				value: w_value,
			})).export()  // eslint-disable-line comma-dangle
		)]);

		k_attr.defaultValue(k_container);

		return this;
	}

	add({type:s_type, values:a_values}, s_key, s_label) {
		let w_value = a_values[0];

		// special handling for null
		if(null === w_value) {
			switch(s_type) {
				case 'string': w_value = ''; break;
				case 'integer':
				case 'real': w_value = 0; break;
				case 'boolean': w_value = false; break;
				default: break;
			}
		}

		// special handling for relation
		if('relation' === s_type) return this.add_relation(s_key, s_label, w_value);

		// all others are literals
		const k_attr = this.attribute(s_key, s_label, H_TYPES[s_type]);
		k_attr.defaultValue(new Literal(this._k_factory, k_attr, '', {
			type: 'Literal'+s_type[0].toUpperCase()+s_type.slice(1),
			value: w_value,
		}));

		return this;
	}

	add_null(s_key, s_label) {
		const k_attr = this.attribute(s_key, s_label, H_TYPES.null);
		const k_default = new Literal(this._k_factory, k_attr, '', {
			type: 'LiteralNull',
		});
		delete k_default._h_export.value;
		k_attr.defaultValue(k_default);

		return this;
	}

	// add_strings(h_add) {
	// 	for(const [si_title, z_value] of Object.entries(h_add)) {
	// 		this.add_string(si_title.replace(/ \w/, s => s.toLowerCase()), si_title, z_value instanceof Set? first(z_value): z_value);
	// 	}
	// }

	add_relation(s_key, s_label, si_target) {
		const si_self = this.id;
		const si_assoc = sha256_hex(`association:${s_key}:${si_self < si_target? si_self+'.'+si_target: si_target+'.'+si_self}`);
		const k_assoc = new Association(this._k_factory, si_assoc, s_label, si_target);
		this._a_baggage.push(k_assoc);

		const k_attr = this.attribute(s_key, s_label, si_target);
		k_attr._h_export.associationId = si_assoc;
		return this;
	}

	// add_links(s_key, s_label, a_links) {
	// 	const a_values = a_links.map(kt => this._k_factory.uri_to_element(kt.value));
	// 	this.add_string_array(s_key, s_label, a_values);
	// }
}


export class Association extends Class {
	constructor(k_factory, si_self, si_owner, si_target) {
		super(k_factory, si_self, si_owner);
		this._si_target = si_target;
	}

	_init() {
		return {
			...super._init(),
			isDerived: false,
			memberEndIds: [this._si_owner, this._si_target],
			ownedEndIds: [],
			navigableOwnedEndIds: [],
		};
	}

	get type() {
		return 'Association';
	}
}


export class ElementFactory {
	constructor(si_project, p_origin) {
		this._si_project = si_project;
		this._p_origin = p_origin;
	}

	get projectId() {
		return this._si_project;
	}

	create_class(si_class, s_name) {
		return new Class(this, si_class, s_name);
	}

	uri_to_element(p_uri) {
		const d_url = new URL(p_uri);

		if(this._p_origin !== d_url.origin) {
			throw new Error(`Cannot convert URI <${p_uri}> to element id; resource is not on same origin '${this._p_origin}'`);
		}

		return d_url.pathname.replace(/\//g, '_');
	}

	origin_matches(p_test) {
		return this._p_origin === (new URL(p_test)).origin;
	}
}
