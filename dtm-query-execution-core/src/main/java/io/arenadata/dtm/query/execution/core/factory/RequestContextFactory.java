package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import org.apache.calcite.sql.SqlNode;

public interface RequestContextFactory<Context extends RequestContext<? extends DatamartRequest>, Request extends QueryRequest> {

	Context create(Request request, SqlNode node);

}
