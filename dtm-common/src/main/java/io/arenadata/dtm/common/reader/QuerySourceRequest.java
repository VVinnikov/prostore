package io.arenadata.dtm.common.reader;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.*;
import org.apache.commons.lang3.SerializationUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Modified ExecutionQueryRequest without hint
 * */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QuerySourceRequest {
    @NonNull
    private QueryRequest queryRequest;
    private List<Datamart> logicalSchema;
    private List<ColumnMetadata> metadata;
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
