package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.Data;

@Data
public abstract class RequestContext<Request extends DatamartRequest> {
	private RequestMetrics metrics;
	private Request request;

	public RequestContext(Request request) {
		this.request = request;
	}

	public RequestContext(RequestMetrics metrics, Request request) {
		this.metrics = metrics;
		this.request = request;
	}

	public Request getRequest() {
		return request;
	}

	public RequestMetrics getMetrics() {
		return metrics;
	}

	public void setMetrics(RequestMetrics metrics) {
		this.metrics = metrics;
	}

	public abstract SqlProcessingType getProcessingType();

}
