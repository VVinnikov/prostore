package io.arenadata.dtm.query.execution.plugin.api.eddl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.ToString;

import static io.arenadata.dtm.common.model.SqlProcessingType.EDDL;

@ToString
public class EddlRequestContext extends RequestContext<DatamartRequest> {

	public EddlRequestContext(RequestMetrics metrics, DatamartRequest request) {
		super(metrics, request);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return EDDL;
	}
}
