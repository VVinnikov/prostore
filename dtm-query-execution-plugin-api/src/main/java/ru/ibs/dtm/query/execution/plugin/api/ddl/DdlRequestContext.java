package ru.ibs.dtm.query.execution.plugin.api.ddl;

import lombok.ToString;
import org.apache.calcite.sql.SqlNode;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import java.util.List;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.UNKNOWN;
import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.DDL;

@ToString
public class DdlRequestContext extends RequestContext<DdlRequest> {

	private DdlType ddlType;
	private Long datamartId;
	private SqlNode query;

	public DdlRequestContext(final DdlRequest request) {
		this(request, null);
	}

	public DdlRequestContext(final DdlRequest request, final SqlNode query) {
		super(request);
		this.ddlType = UNKNOWN;
		this.query = query;
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

	public Long getDatamartId() {
		return datamartId;
	}

	public void setDatamartId(Long datamartId) {
		this.datamartId = datamartId;
	}

	public SqlNode getQuery() {
		return query;
	}

}
