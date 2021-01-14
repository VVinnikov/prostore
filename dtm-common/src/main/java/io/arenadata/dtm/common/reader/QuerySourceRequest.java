package io.arenadata.dtm.common.reader;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.*;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Modified ExecutionQueryRequest without hint
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QuerySourceRequest {
    @NonNull
    private QueryRequest queryRequest;
    private List<Datamart> logicalSchema;
    private List<ColumnMetadata> metadata;
    private QueryTemplateResult queryTemplate;
    private SourceType sourceType;
    @NonNull
    private SqlNode query;

    public QuerySourceRequest(@NonNull QueryRequest queryRequest, @NonNull SqlNode query, SourceType sourceType) {
        this.queryRequest = queryRequest;
        this.sourceType = sourceType;
        this.query = query;
    }

    public List<Datamart> getLogicalSchema() {
        return logicalSchema.stream()
                .map(Datamart::copy)
                .collect(Collectors.toList());
    }
}
