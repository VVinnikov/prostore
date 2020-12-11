package io.arenadata.dtm.common.reader;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

/**
 * The result of the query execution.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryResult {
    private UUID requestId;
    private List<Map<String, Object>> result = new ArrayList<>();
    private List<ColumnMetadata> metadata;
    private String timeZone;

    public QueryResult(UUID requestId, List<Map<String, Object>> result) {
        this.requestId = requestId;
        this.result = result;
    }

    public static QueryResult emptyResult() {
        return new EmptyQueryResult();
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
