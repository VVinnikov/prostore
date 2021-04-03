package io.arenadata.dtm.query.execution.core.delta.dto.operation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WriteOpFinish {
    private String tableName;
    private List<Long> cnList;
}
