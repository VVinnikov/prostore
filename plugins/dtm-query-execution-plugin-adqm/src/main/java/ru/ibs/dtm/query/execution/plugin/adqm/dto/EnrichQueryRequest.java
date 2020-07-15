package ru.ibs.dtm.query.execution.plugin.adqm.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

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
