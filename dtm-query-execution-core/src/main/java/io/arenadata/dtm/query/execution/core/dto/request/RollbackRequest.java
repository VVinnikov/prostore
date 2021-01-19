package io.arenadata.dtm.query.execution.core.dto.request;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.request.DatamartRequest;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class RollbackRequest extends DatamartRequest {

    private String datamart;
    private String destinationTable;
    private long sysCn;
    private Entity entity;

    @Builder
    public RollbackRequest(QueryRequest queryRequest,
                           String datamart,
                           String destinationTable,
                           long sysCn,
                           Entity entity) {
        super(queryRequest);
        this.datamart = datamart;
        this.destinationTable = destinationTable;
        this.sysCn = sysCn;
        this.entity = entity;
    }
}
