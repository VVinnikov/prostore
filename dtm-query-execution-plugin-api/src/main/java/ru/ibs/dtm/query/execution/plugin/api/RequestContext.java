package ru.ibs.dtm.query.execution.plugin.api;

import lombok.Data;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

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
