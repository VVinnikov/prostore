package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import lombok.Data;

@Data
public abstract class RequestContext<Request extends DatamartRequest> {

	private Request request;

	public RequestContext(Request request) {
		this.request = request;
	}

	public Request getRequest() {
		return request;
	}

	public abstract SqlProcessingType getProcessingType();

}
