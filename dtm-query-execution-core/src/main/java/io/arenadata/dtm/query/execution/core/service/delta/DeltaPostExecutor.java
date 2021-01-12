package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.PostExecutor;
import org.springframework.beans.factory.annotation.Autowired;

public interface DeltaPostExecutor extends PostExecutor<DeltaRequestContext> {
    @Autowired
    default void register(DeltaService<QueryResult> service) {
        service.addPostExecutor(this);
    }
}
