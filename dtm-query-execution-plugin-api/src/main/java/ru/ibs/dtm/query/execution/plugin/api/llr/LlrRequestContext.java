package ru.ibs.dtm.query.execution.plugin.api.llr;

import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.LLR;

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
