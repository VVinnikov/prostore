package ru.ibs.dtm.query.execution.plugin.api.ddl;

import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;

@ToString
public class DdlRequestContext extends RequestContext<DdlRequest> {

	public DdlRequestContext(DdlRequest ddlRequest) {
		super(ddlRequest);

	}
}
