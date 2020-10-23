package ru.ibs.dtm.query.execution.core.dto.delta;

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
