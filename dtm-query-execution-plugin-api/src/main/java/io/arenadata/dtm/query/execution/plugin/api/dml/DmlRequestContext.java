package io.arenadata.dtm.query.execution.plugin.api.dml;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import static io.arenadata.dtm.common.model.SqlProcessingType.DML;

@ToString
public class DmlRequestContext extends RequestContext<DatamartRequest> {

	private SqlNode query;

	public DmlRequestContext(RequestMetrics metrics, DatamartRequest request, SqlNode query) {
		super(metrics, request);
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
