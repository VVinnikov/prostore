package io.arenadata.dtm.query.execution.plugin.api.service.ddl;

import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

public interface CreateTableQueriesFactory<T> {
    T create(DdlRequestContext context);
}
