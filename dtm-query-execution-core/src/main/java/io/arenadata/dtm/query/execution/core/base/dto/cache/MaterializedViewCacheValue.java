package io.arenadata.dtm.query.execution.core.base.dto.cache;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MaterializedViewCacheValue {
    private final Entity entity;
    private final int status;
    private final long failsCount;

    public MaterializedViewCacheValue(Entity entity) {
        this.entity = entity;
        this.status = 0;
        this.failsCount = 0;
    }
}
