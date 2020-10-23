package io.arenadata.dtm.common.dto;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class QueryParserResponse {
    private final CalciteContext calciteContext;
    private final QueryRequest queryRequest;
    private final List<Datamart> schema;
    private final RelRoot relNode;
    private final SqlNode sqlNode;
}
