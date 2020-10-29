import TurtleReader from '@graphy/content.ttl.read';
import FastDataset from '@graphy/memory.dataset.fast';
import {pipeline as _pipeline} from 'stream';
import {promisify} from 'util';
const pipeline = promisify(_pipeline);

import {
	MmsUmlJsonTranslator,
} from '../class/mms-uml-json-translator.mjs';

import H_PREFIXES from '../common/prefixes.mjs';

export async function dng_translate(gc_translate) {
	// verbose
	console.log('loading dataset into memory...');
	console.time('load');

	// prep project dataset
	const kd_project = FastDataset();

	// prep prefixes
	let h_prefixes;

	// read exported dataset
	await pipeline(...[
		gc_translate.local_exported,
		new TurtleReader({
			eof(_h_prefixes) {
				h_prefixes = {
					..._h_prefixes,
					...H_PREFIXES,
				};
			},
		}),
		// load into memory
		kd_project,
	]);

	// perf
	console.timeEnd('load');
	console.log('translating...');
	console.time('translate');

	// create translator
	const k_translator = new MmsUmlJsonTranslator({
		...gc_translate,
		dng_prefixes: h_prefixes,
		mem_dataset: kd_project,
		local_output: gc_translate.local_adds,
	});

	// translate artifacts
	k_translator.translate_artifacts(true);

	// close output
	await k_translator.end();

	// done
	console.timeEnd('translate');
}

export default dng_translate;
