import chalk from 'chalk';
const cherr = chalk.stderr;

export class SkipError extends Error {}

export class HttpError extends Error {
	constructor(gc_error) {
		super(gc_error._s_message);
		({
			url: this._s_url,
			message: this._s_message,
			status: this._nc_status,
			headers: this._h_headers,
			body: this._s_body,
		} = gc_error);
	}

	get message() {
		return cherr.red(`${this._nc_status} response from '${this._s_url}'; Content-Type: ${this._h_headers['content-type']}`)
			+cherr.yellow(`${JSON.stringify(this._h_headers)}\n${this._s_body}`);
	}
}

