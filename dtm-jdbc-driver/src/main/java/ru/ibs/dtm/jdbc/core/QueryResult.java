package ru.ibs.dtm.jdbc.core;

import java.util.List;
import java.util.Map;
import lombok.Data;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

/**
 * Result of executing sql-query
 */
@Data
public class QueryResult {

    /**
     * Request identifier
     */
    private String requestId;

    /**
     * Query result List<Map<ColumnName, ColumnValue>>
     */
    private List<Map<String, Object>> result;

    /**
     * Is query result empty
     */
    private boolean empty;

    /**
     * Query result metadata
     */
    private List<ColumnMetadata> metadata;
}
