package io.arenadata.dtm.common.dto;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Data;

import java.util.List;

@Data
public class QueryParserRequest {
    private final QueryRequest queryRequest;
    private final List<Datamart> schema;
}
