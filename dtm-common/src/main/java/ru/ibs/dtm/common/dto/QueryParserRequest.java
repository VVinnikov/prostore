package ru.ibs.dtm.common.dto;

import lombok.Data;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

@Data
public class QueryParserRequest {
    private QueryRequest queryRequest;
    private Datamart schema;
}
