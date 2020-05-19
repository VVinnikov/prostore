package ru.ibs.dtm.query.execution.plugin.api.ddl;

import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;

@ToString
public class DdlRequestContext extends RequestContext<DdlRequest, DdlQueryType> {

	public DdlRequestContext(DdlRequest ddlRequest, DdlQueryType ddlQueryType) {
		super(ddlRequest, ddlQueryType);

	}
}
