package io.arenadata.dtm.query.execution.core.base.dto.cache;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

@Data
@AllArgsConstructor
public class MaterializedViewCacheValue {
    private final Entity entity;
    private UUID uuid;
    private long failsCount;
    private MaterializedViewSyncStatus status;

    public MaterializedViewCacheValue(Entity entity) {
        this.entity = entity;
        this.uuid = UUID.randomUUID();
        this.status = MaterializedViewSyncStatus.READY;
        this.failsCount = 0;
    }

    public void incrementFailsCount() {
        failsCount++;
    }

    public void markForDeletion() {
        this.uuid = null;
    }
}
