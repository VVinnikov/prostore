package ru.ibs.dtm.query.execution.core.dto.delta;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeltaWriteOpRequest {
    private String datamart;
    private String tableName;
    private String tableNameExt;
    private String query;
}