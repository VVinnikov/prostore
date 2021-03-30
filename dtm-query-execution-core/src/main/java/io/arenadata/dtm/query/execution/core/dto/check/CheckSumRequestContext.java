package io.arenadata.dtm.query.execution.core.dto.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Data
@Builder
public class CheckSumRequestContext {
    private CheckContext checkContext;
    private String datamart;
    private OkDelta delta;
    private Entity entity;
    private Set<String> columns;

    public CheckSumRequestContext copy() {
        return CheckSumRequestContext.builder()
                .checkContext(checkContext)
                .datamart(datamart)
                .delta(delta)
                .entity(entity)
                .columns(columns)
                .build();
    }
}
