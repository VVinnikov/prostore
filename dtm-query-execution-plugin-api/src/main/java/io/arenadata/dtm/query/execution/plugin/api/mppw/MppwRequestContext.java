package io.arenadata.dtm.query.execution.plugin.api.mppw;

import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import lombok.ToString;

import static io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType.MPPW;

@ToString
public class MppwRequestContext extends RequestContext<MppwRequest> {

	public MppwRequestContext(MppwRequest request) {
		super(request);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return MPPW;
	}
}
