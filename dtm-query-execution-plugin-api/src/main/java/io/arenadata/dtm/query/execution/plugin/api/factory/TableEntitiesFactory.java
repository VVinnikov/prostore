package io.arenadata.dtm.query.execution.plugin.api.factory;

import io.arenadata.dtm.common.model.ddl.Entity;

public interface TableEntitiesFactory<T> {
    T create(Entity entity, String envName);
}
