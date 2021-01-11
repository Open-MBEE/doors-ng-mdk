const P_SORT_BY_PREDICATE = 'http://purl.org/dc/terms/identifier';

export async function dng_folder(k_client, si_project, p_folder, n_pagintation_size) {
	// accumulate artifacts
	let a_artifacts = [];

	// owl:sameAs dcterms:identifier
	// const P_SORT_BY_PREDICATE = `${k_client._p_server}/rm/types/_ye4_ltP1EeSkXuceQrR01w`;

	// paginated iteration
	for(let i_page=1; ; i_page++) {
		// issue request
		const ds_res = await k_client.request('/rm/views?'+(new URLSearchParams({
			execute: true,
			fullObject: false,
			size: n_pagintation_size,
			count: true,
			page: i_page,
			reuse: false,
			extrinsicReuse: false,
		})), {
			method: 'POST',
			headers: {
				accept: 'text/json',
				'content-type': 'text/plain',
				'doorsrp-request-type': 'private',
				'net.jazz.jfs.owning-context': `${k_client._p_server}/rm/rm-projects/${si_project}`,
				// 'vvc.configuration': `${k_client._p_server}/rm/cm/stream/${si_stream}`,
			},
		}, `
			<rdf:RDF
				xmlns:dcterms="http://purl.org/dc/terms/"
				xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
				xmlns:rs="http://www.w3.org/2001/sw/DataAccess/tests/result-set#"
				xmlns:rrmNav="http://com.ibm.rdm/navigation#"
				xmlns:rrmViewdata="http://com.ibm.rdm/viewdata#"
				xmlns:rm="http://www.ibm.com/xmlns/rdm/rdf/"
				xmlns:xsd="http://www.w3.org/2001/XMLSchema#"
				xmlns:rql="http://www.ibm.com/xmlns/rdm/rql/"
				xmlns:owl="http://www.w3.org/2002/07/owl#">
				<rm:View rdf:about="">
					<rm:rowquery rdf:parseType="Resource">
						<rql:select rdf:parseType="Resource">
							<rdf:_1 rdf:parseType="Resource">
								<rql:object>R1</rql:object>
							</rdf:_1>
						</rql:select>
						<rql:where rdf:parseType="Resource">
							<rdf:_1 rdf:parseType="Resource">
								<rql:e1 rdf:parseType="Resource">
									<rql:field rdf:resource="http://com.ibm.rdm/navigation#parent"/>
									<rql:object>R1</rql:object>
								</rql:e1>
								<rql:e2>
									<rdf:Seq>
										<rdf:li rdf:resource="${p_folder}"/>
									</rdf:Seq>
								</rql:e2>
								<rql:op>in</rql:op>
							</rdf:_1>
						</rql:where>
						<rql:sort rdf:parseType="Resource">
							<rdf:_1 rdf:parseType="Resource">
								<rql:objField rdf:parseType="Resource">
									<rql:field rdf:resource="${P_SORT_BY_PREDICATE}"/>
									<rql:object>R1</rql:object>
								</rql:objField>
								<rql:order>desc</rql:order>
							</rdf:_1>
						</rql:sort>
					</rm:rowquery>
					<rm:displayBaseProperties rdf:datatype="http://www.w3.org/2001/XMLSchema#boolean">true</rm:displayBaseProperties>
					<rrmNav:scope>public</rrmNav:scope>
					<rm:ofType>GridView</rm:ofType>
					<dcterms:description> </dcterms:description>
					<dcterms:title>Grid View 1</dcterms:title>
				</rm:View>
			</rdf:RDF>
		`);

		// read response
		let s_body = '';
		for await(const s_chunk of ds_res) {
			s_body += s_chunk;
		}

		// try parsing JSON
		let g_json;
		try {
			g_json = JSON.parse(s_body);
		}
		catch(e_parse) {
			throw new Error(`DNG sent back a response from a folder request that is not JSON: '''\n${s_body}\n'''`);
		}

		// iterate results
		let c_rows = 0;
		for(const [s_key, g_entry] of Object.entries(g_json.feed)) {
			if(s_key.startsWith('entry')) {
				c_rows += 1;
				for(const [, g_binding] of Object.entries(g_entry.content.result)) {
					if('R1' === g_binding.xmlAttributes?.name) {
						a_artifacts.push(g_binding.uri.value);
						continue;
					}
				}
			}
		}

		// finished scraping
		if(c_rows < n_pagintation_size) {
			break;
		}
	}

	return a_artifacts;
}

export default dng_folder;
