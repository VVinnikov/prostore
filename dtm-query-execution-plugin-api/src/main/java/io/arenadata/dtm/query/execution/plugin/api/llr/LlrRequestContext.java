package io.arenadata.dtm.query.execution.plugin.api.llr;

import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import lombok.ToString;

import static io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType.LLR;

@ToString
public class LlrRequestContext extends RequestContext<LlrRequest> {

	public LlrRequestContext(LlrRequest request) {
		super(request);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return LLR;
	}
}
