package io.arenadata.dtm.query.execution.plugin.api.mppr;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.ToString;

import static io.arenadata.dtm.common.model.SqlProcessingType.MPPR;

@ToString
public class MpprRequestContext extends RequestContext<MpprRequest> {

	public MpprRequestContext(RequestMetrics metrics, MpprRequest request) {
		super(metrics, request);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return MPPR;
	}
}
