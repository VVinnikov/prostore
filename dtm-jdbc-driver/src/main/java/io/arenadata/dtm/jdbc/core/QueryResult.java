package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import lombok.Data;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/**
 * Sql query response
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
     * List of system metadata
     */
    private List<ColumnMetadata> metadata;

    /**
     * Time Zone of ResultSet
     */
    private String timeZone;
}
