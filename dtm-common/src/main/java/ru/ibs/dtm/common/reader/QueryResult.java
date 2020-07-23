package ru.ibs.dtm.common.reader;

import io.vertx.core.json.JsonArray;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import lombok.Data;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

/**
 * Результат выполнения запроса.
 */
@Data
public class QueryResult {
    private static final QueryResult emptyResult = new QueryResult(null, new JsonArray());
    private UUID requestId;
    private JsonArray result;
    private List<ColumnMetadata> metadata;

    public QueryResult(UUID requestId, JsonArray result) {
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
