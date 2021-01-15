package io.arenadata.dtm.query.execution.plugin.api.eddl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import static io.arenadata.dtm.common.model.SqlProcessingType.EDDL;

@ToString
public class EddlRequestContext extends RequestContext<DatamartRequest, SqlNode> {

	public EddlRequestContext(RequestMetrics metrics,
							  DatamartRequest request,
							  String envName,
							  SqlNode sqlNode) {
		super(request, sqlNode, envName, metrics);
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return EDDL;
	}
}
