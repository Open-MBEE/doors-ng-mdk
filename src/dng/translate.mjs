import TurtleReader from '@graphy/content.ttl.read';
import FastDataset from '@graphy/memory.dataset.fast';
import {pipeline as _pipeline} from 'stream';
import {promisify} from 'util';
const pipeline = promisify(_pipeline);

import {
	MmsUmlJsonTranslator,
} from '../class/mms-uml-json-translator.mjs';

import H_PREFIXES from '../common/prefixes.mjs';

// eslint-disable-next-line no-console
const d_console = new console.Console({
	stdout: process.stderr,
});

export async function dng_translate(gc_translate) {
	// verbose
	d_console.warn('loading dataset into memory...');
	d_console.time('load');

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
	d_console.timeEnd('load');
	d_console.log('translating...');
	d_console.time('translate');

	// create translator
	const k_translator = new MmsUmlJsonTranslator({
		...gc_translate,
		dng_prefixes: h_prefixes,
		mem_dataset: kd_project,
		local_output: gc_translate.local_adds,
	});

	// translate artifacts
	k_translator.translate_artifacts(true);

	// translate modules
	k_translator.translate_modules();

	// translate folders
	k_translator.translate_folders();

	// close output
	await k_translator.end();

	// done
	d_console.timeEnd('translate');
}

export default dng_translate;
