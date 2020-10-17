import DataFactory from '@graphy/core.data.factory';
import TurtleReader from '@graphy/content.ttl.read';
import TurtleWriter from '@graphy/content.ttl.write';
import FastDataset from '@graphy/memory.dataset.fast';

const {
	c1,
	namedNode,
} = DataFactory;

const KT_RDF_TYPE = c1('a');

import H_PREFIXES from '../common/prefixes.mjs';

// dataset
const k_dataset = FastDataset();

// prep prefixes
let h_prefixes;

// read input
process.stdin.pipe(new TurtleReader({
	eof(_h_prefixes) {
		h_prefixes = {
			..._h_prefixes,
			...H_PREFIXES,
		};
	},
}))
	.pipe(k_dataset)
	.on('finish', () => {
		// prep output
		const ds_out = new TurtleWriter({
			prefixes: h_prefixes,
		});

		// pipe to stdout
		ds_out.pipe(process.stdout);

		// select all requirements
		const kd_req_types = k_dataset.match(null, KT_RDF_TYPE, c1('oslc_rm:Requirement', H_PREFIXES));

		// 
		const a_reqs = [];
		const as_preds = new Set();
		const as_links = new Set();

		// each requirement
		for(const kq_req_type of kd_req_types) {
			// pull out all triples pertaining to this subject
			const kd_req = k_dataset.match(kq_req_type.subject, null, null);

			// write all quads belong to this requirement to output
			ds_out.write({
				type: 'c3',
				value: kd_req._h_quad_tree['*'],
			});

			// push to requirement list
			a_reqs.push(kq_req_type.subject.value);

			// each quad for this subject
			for(const kq_req of kd_req) {
				// push predicates to set
				as_preds.add(kq_req.predicate.value);

				// named node; push object to set
				const kt_object = kq_req.object;
				if(kt_object.isNamedNode) {
					as_links.add(kt_object.value);
				}
			}
		}

		// remove requirements from links
		for(const p_req of a_reqs) {
			as_links.delete(p_req);
		}

		// each link
		for(const p_link of as_links) {
			// search vocabulary
			const kd_preds = k_dataset.match(namedNode(p_link), null, null);

			// write to ouotput
			ds_out.write({
				type: 'c3',
				value: kd_preds._h_quad_tree['*'],
			});
		}

		// each predicate
		for(const p_predicate of as_preds) {
			// search vocabulary
			const kd_preds = k_dataset.match(namedNode(p_predicate), null, null);

			// write to ouotput
			ds_out.write({
				type: 'c3',
				value: kd_preds._h_quad_tree['*'],
			});
		}
	});
