package ru.ibs.dtm.query.execution.plugin.api.ddl;

import lombok.Data;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType.UNKNOWN;
import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.DDL;

@Data
@ToString
public class DdlRequestContext extends RequestContext<DdlRequest> {

	private DdlType ddlType;
	private String datamartName;
	private Long datamartId;
	private SqlNode query;
	private String systemName;

	public DdlRequestContext(final DdlRequest request) {
		this(request, null);
	}

	public DdlRequestContext(final DdlRequest request, final SqlNode query) {
		super(request);
		this.ddlType = UNKNOWN;
		this.query = query;
		this.systemName = "local";
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return DDL;
	}

}
