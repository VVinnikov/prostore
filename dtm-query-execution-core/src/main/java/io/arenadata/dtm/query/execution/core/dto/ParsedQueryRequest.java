package io.arenadata.dtm.query.execution.core.dto;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
