package ru.ibs.dtm.common.dto;

import lombok.Data;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

@Data
public class QueryParserRequest {
    private final QueryRequest queryRequest;
    private final List<Datamart> schema;
}
