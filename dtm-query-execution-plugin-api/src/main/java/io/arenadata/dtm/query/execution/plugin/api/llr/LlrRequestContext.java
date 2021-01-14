package io.arenadata.dtm.query.execution.plugin.api.llr;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import lombok.ToString;

import java.util.List;

import static io.arenadata.dtm.common.model.SqlProcessingType.LLR;

@ToString
public class LlrRequestContext extends RequestContext<LlrRequest> {

	private List<DeltaInformation> deltaInformations;

	public LlrRequestContext(RequestMetrics metrics, LlrRequest request) {
		super(request, sqlNode, envName, sourceType, metrics);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return LLR;
	}
}
