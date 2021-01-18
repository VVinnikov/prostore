package io.arenadata.dtm.query.execution.plugin.api.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

public interface CreateTableQueriesFactory<T> {
    T create(Entity entity, String envName);
}
