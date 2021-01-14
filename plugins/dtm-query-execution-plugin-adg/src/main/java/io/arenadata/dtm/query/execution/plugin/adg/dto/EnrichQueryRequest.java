package io.arenadata.dtm.query.execution.plugin.adg.dto;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrichQueryRequest {
    private QueryRequest queryRequest;
    private List<Datamart> schema;
    private SqlNode query;

    public static EnrichQueryRequest generate(QueryRequest queryRequest, List<Datamart> schema, SqlNode query) {
        return new EnrichQueryRequest(queryRequest, schema, query);
    }
}
