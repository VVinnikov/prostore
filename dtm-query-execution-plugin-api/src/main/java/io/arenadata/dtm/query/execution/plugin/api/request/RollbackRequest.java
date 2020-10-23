package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class RollbackRequest extends DatamartRequest {

    private String datamart;
    private String targetTable;
    private long sysCn;
    private Entity entity;

    @Builder
    public RollbackRequest(QueryRequest queryRequest, String datamart, String targetTable, long sysCn, Entity entity) {
        super(queryRequest);
        this.datamart = datamart;
        this.targetTable = targetTable;
        this.sysCn = sysCn;
        this.entity = entity;
    }
}
