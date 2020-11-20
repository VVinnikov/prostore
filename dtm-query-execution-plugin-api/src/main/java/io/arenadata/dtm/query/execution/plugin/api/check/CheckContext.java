package io.arenadata.dtm.query.execution.plugin.api.check;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;

public class CheckContext extends RequestContext<DatamartRequest> {
    private Entity entity;

    public CheckContext(DatamartRequest request) {
        super(request);
    }

    public CheckContext(DatamartRequest request, Entity entity) {
        super(request);
        this.entity = entity;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.CHECK;
    }

    public Entity getEntity() {
        return entity;
    }

    public void setEntity(Entity entity) {
        this.entity = entity;
    }
}
