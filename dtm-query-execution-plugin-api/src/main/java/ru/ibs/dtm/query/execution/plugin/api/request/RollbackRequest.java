package ru.ibs.dtm.query.execution.plugin.api.request;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.reader.QueryRequest;

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
