package ru.ibs.dtm.query.execution.plugin.api.eddl;

import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.EDDL;

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
