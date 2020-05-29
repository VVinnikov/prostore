package ru.ibs.dtm.query.execution.plugin.api.edml;

import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.EDML;

@ToString
public class EdmlRequestContext extends RequestContext<DatamartRequest> {

	public EdmlRequestContext(DatamartRequest request) {
		super(request);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return EDML;
	}
}
