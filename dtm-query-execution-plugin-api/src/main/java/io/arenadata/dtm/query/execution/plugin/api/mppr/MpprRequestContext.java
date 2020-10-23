package io.arenadata.dtm.query.execution.plugin.api.mppr;

import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import lombok.ToString;

import static io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType.MPPR;

@ToString
public class MpprRequestContext extends RequestContext<MpprRequest> {

	public MpprRequestContext(MpprRequest request) {
		super(request);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return MPPR;
	}
}
