package io.arenadata.dtm.common.dto;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class QueryParserRequest {
    private final SqlNode query;
    private final List<Datamart> schema;
}
