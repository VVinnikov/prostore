package io.arenadata.dtm.query.execution.plugin.api.service.mppw;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.vertx.core.Future;

public interface MppwExecutor {
    Future<QueryResult> execute(MppwRequest request);

    ExternalTableLocationType getType();
}
