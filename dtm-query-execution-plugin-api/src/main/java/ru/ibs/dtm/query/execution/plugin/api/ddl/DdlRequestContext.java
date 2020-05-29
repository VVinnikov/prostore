package ru.ibs.dtm.query.execution.plugin.api.ddl;

import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.UNKNOWN;
import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.DDL;

@ToString
public class DdlRequestContext extends RequestContext<DdlRequest> {

	private DdlType ddlType;

	public DdlRequestContext(DdlRequest request) {
		super(request);
		ddlType = UNKNOWN;
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return DDL;
	}

	public DdlType getDdlType() {
		return ddlType;
	}

	public void setDdlType(DdlType ddlType) {
		this.ddlType = ddlType;
	}
}
