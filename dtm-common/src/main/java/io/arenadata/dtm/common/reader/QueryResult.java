package io.arenadata.dtm.common.reader;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import lombok.Data;

import java.util.*;

/**
 * Результат выполнения запроса.
 */
@Data
public class QueryResult {
    private static final QueryResult emptyResult = new QueryResult(null, new ArrayList<>());
    private UUID requestId;
    private List<Map<String, Object>> result;
    private List<ColumnMetadata> metadata;

    public QueryResult(UUID requestId, List<Map<String, Object>> result, List<ColumnMetadata> metadata) {
        this.requestId = requestId;
        this.result = result;
        this.metadata = metadata;
    }

    public QueryResult(UUID requestId, List<Map<String, Object>> result) {
        this.requestId = requestId;
        this.result = result;
    }

    public QueryResult() {
    }

    public static QueryResult emptyResult() {
        return emptyResult;
    }

    public boolean isEmpty() {
        return result == null || result.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryResult result1 = (QueryResult) o;
        return Objects.equals(getRequestId(), result1.getRequestId()) &&
                Objects.equals(getResult(), result1.getResult());
    }
}
