package ru.ibs.dtm.query.execution.plugin.api.dml;

import lombok.ToString;
import org.apache.calcite.sql.SqlNode;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.DML;

@ToString
public class DmlRequestContext extends RequestContext<DatamartRequest> {

	private SqlNode query;

	public DmlRequestContext(final DatamartRequest request) {
		this(request, null);
	}

	public DmlRequestContext(final DatamartRequest request, final SqlNode query) {
		super(request);
		this.query = query;
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return DML;
	}

	public SqlNode getQuery() {
		return query;
	}
}
