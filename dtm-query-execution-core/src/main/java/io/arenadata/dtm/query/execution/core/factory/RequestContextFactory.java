package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import org.apache.calcite.sql.SqlNode;

public interface RequestContextFactory<Context extends RequestContext<? extends DatamartRequest, ? extends SqlNode>, Request extends QueryRequest> {

	Context create(Request request, SourceType sourceType, SqlNode node);

}
