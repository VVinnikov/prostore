package ru.ibs.dtm.query.execution.plugin.api;

import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

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
