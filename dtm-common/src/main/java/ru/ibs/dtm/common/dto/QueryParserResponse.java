package ru.ibs.dtm.common.dto;

import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

@Data
public class QueryParserResponse {
    private final CalciteContext calciteContext;
    private final QueryRequest queryRequest;
    private final List<Datamart> schema;
    private final RelRoot relNode;
    private final SqlNode sqlNode;
}
