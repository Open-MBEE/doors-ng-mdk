import got from 'got';
import HttpAgent from 'agentkeepalive';
const HttpsAgent = HttpAgent.HttpsAgent;

const N_MAX_REQUESTS = parseInt(process.env.HTTP_MAX_REQUESTS || 128);

export class HttpClient {
	constructor(gc_client={}) {
		let {
			max_requests: n_max_requests=N_MAX_REQUESTS,
			base_url: p_base='',
		} = gc_client;

		this._f_request = got.extend({
			prefixUrl: p_base || '',
			agent: {
				// http: new HttpAgent(),
				https: new HttpsAgent({
					maxSockets: n_max_requests,
				}),
			},
		});

		this._n_max_requests = n_max_requests;
		this._a_queue = [];
		this._c_requests = 0;
	}

	stream(g_request) {
		return this._f_request.stream(g_request);
	}

	request(g_request) {
		return this._f_request(g_request);
	}
}

Object.assign(HttpClient, {
	agent: new HttpsAgent(),
});

export default HttpClient;
