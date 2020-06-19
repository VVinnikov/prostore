package ru.ibs.dtm.query.execution.plugin.api.mppr;

import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.*;

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
