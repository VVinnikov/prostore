package io.arenadata.dtm.query.execution.plugin.api.service.ddl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.PostExecutor;
import org.springframework.beans.factory.annotation.Autowired;

public interface DdlPostExecutor extends PostExecutor<DdlRequestContext> {
    @Autowired
    default void register(DdlService<QueryResult> service) {
        service.addPostExecutor(this);
    }
}
