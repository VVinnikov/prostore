package io.arenadata.dtm.query.execution.plugin.api.eddl;

import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import lombok.ToString;

import static io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType.EDDL;

@ToString
public class EddlRequestContext extends RequestContext<DatamartRequest> {

	public EddlRequestContext(DatamartRequest request) {
		super(request);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return EDDL;
	}
}
