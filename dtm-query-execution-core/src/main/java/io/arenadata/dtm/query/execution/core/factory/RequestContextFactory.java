package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.core.dto.CoreRequestContext;
import org.apache.calcite.sql.SqlNode;

public interface RequestContextFactory<Context extends CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>, Request extends QueryRequest> {

    Context create(Request request, SqlNode node);

}
