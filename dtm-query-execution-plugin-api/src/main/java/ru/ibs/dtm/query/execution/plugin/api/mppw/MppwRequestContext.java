package ru.ibs.dtm.query.execution.plugin.api.mppw;

import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.MPPR;
import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.MPPW;

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
