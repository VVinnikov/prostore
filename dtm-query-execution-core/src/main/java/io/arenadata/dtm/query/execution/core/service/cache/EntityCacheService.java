package io.arenadata.dtm.query.execution.core.service.cache;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.service.cache.key.EntityKey;

public interface EntityCacheService extends CacheService<EntityKey, Entity> {
    default void remove(String datamartName, String entityName) {
        remove(new EntityKey(datamartName, entityName));
    }
}
