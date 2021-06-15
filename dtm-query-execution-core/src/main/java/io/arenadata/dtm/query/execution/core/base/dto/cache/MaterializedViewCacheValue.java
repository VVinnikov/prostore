package io.arenadata.dtm.query.execution.core.base.dto.cache;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

@Data
@AllArgsConstructor
public class MaterializedViewCacheValue {
    private Entity entity;
    private UUID uuid;
    private long failsCount;
    private int status;

    public MaterializedViewCacheValue(Entity entity) {
        this.entity = entity;
        this.uuid = UUID.randomUUID();
        this.status = 0;
        this.failsCount = 0;
    }

    public void incrementFailsCount() {
        failsCount++;
    }
}
