package io.arenadata.dtm.query.execution.core.check.dto;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Data
@Builder
public class CheckSumRequestContext {
    private CheckContext checkContext;
    private String datamart;
    private Long deltaNum;
    private long cnFrom;
    private long cnTo;
    private Entity entity;
    private Set<String> columns;

    public CheckSumRequestContext copy() {
        return CheckSumRequestContext.builder()
                .checkContext(checkContext)
                .datamart(datamart)
                .deltaNum(deltaNum)
                .cnFrom(cnFrom)
                .cnTo(cnTo)
                .entity(entity)
                .columns(columns)
                .build();
    }
}
