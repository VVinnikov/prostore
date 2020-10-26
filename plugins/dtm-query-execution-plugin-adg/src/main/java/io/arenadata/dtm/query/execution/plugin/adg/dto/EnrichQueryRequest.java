package io.arenadata.dtm.query.execution.plugin.adg.dto;

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

    public static EnrichQueryRequest generate(QueryRequest queryRequest, List<Datamart> schema) {
        return new EnrichQueryRequest(queryRequest, schema);
    }
}
