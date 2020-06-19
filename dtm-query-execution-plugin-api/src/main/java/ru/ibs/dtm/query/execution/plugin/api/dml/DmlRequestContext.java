package ru.ibs.dtm.query.execution.plugin.api.dml;

import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.DML;

@ToString
public class DmlRequestContext extends RequestContext<DatamartRequest> {

	public DmlRequestContext(DatamartRequest request) {
		super(request);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return DML;
	}
}
