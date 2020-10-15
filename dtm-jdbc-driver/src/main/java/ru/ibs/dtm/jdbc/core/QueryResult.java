package ru.ibs.dtm.jdbc.core;

import java.util.List;
import java.util.Map;
import lombok.Data;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

/**
 * Sql query response
 */
@Data
public class QueryResult {

    /**
     * RequestId
     */
    private String requestId;

    /**
     * List of result map
     */
    private List<Map<String, Object>> result;

    /**
     * is result empty
     */
    private boolean empty;

    /**
     * List of system metadata
     */
    private List<ColumnMetadata> metadata;
}
