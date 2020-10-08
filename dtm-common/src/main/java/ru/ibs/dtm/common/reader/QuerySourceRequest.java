package ru.ibs.dtm.common.reader;

import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.SerializationUtils;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

/**
 * Modified ExecutionQueryRequest without hint
 * */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QuerySourceRequest {
    @NonNull
    private QueryRequest queryRequest;
    private List<Datamart> logicalSchema;
    private SourceType sourceType;

    public List<Datamart> getLogicalSchema() {
        return logicalSchema.stream()
                .map(SerializationUtils::clone)
                .collect(Collectors.toList());
    }

    public QuerySourceRequest(@NonNull QueryRequest queryRequest, SourceType sourceType) {
        this.queryRequest = queryRequest;
        this.sourceType = sourceType;
    }
}
