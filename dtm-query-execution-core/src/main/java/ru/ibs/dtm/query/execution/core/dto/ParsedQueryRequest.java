package ru.ibs.dtm.query.execution.core.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

/**
 * dto для передачи информации диспетчеру
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ParsedQueryRequest {
    private QueryRequest queryRequest;
    private SqlProcessingType processingType;
}
