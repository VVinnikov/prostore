package io.arenadata.dtm.query.execution.plugin.api.llr;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.ToString;

import static io.arenadata.dtm.common.model.SqlProcessingType.LLR;

@ToString
public class LlrRequestContext extends RequestContext<LlrRequest> {

	public LlrRequestContext(RequestMetrics metrics, LlrRequest request) {
		super(metrics, request);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return LLR;
	}
}
