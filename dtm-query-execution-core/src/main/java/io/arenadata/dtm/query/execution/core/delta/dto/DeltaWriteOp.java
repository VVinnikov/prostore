package io.arenadata.dtm.query.execution.core.delta.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeltaWriteOp {
    private long cnFrom;
    private String tableName;
    private String tableNameExt;
    private String query;
    private int status;
    private Long sysCn;
}
