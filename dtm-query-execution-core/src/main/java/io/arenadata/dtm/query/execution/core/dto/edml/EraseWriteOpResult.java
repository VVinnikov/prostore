package io.arenadata.dtm.query.execution.core.dto.edml;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EraseWriteOpResult {
    private String tableName;
    private long sysCn;
}
