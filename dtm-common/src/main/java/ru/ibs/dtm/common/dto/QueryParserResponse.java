package ru.ibs.dtm.common.dto;

import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

@Data
public class QueryParserResponse {
    private final QueryRequest queryRequest;
    private final Datamart schema;
    private final RelRoot relNode;
    private final SqlNode sqlNode;
}
