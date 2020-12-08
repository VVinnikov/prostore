package io.arenadata.dtm.query.execution.plugin.adqm.dto;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrichQueryRequest {
    private QueryRequest queryRequest;
    private List<Datamart> schema;
    private boolean isLocal;

    public static EnrichQueryRequest generate(QueryRequest queryRequest, List<Datamart> schema, boolean isLocal) {
        return new EnrichQueryRequest(queryRequest, schema, isLocal);
    }

    public static EnrichQueryRequest generate(QueryRequest queryRequest, List<Datamart> schema) {
        return new EnrichQueryRequest(queryRequest, schema, false);
    }
}
