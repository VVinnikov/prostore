package ru.ibs.dtm.query.execution.core.factory;

import org.apache.calcite.sql.SqlNode;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

public interface RequestContextFactory<Context extends RequestContext<? extends DatamartRequest>, Request extends QueryRequest> {

	Context create(Request request, SqlNode node);

}
