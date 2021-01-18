package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Getter
@Setter
public class LlrRequest extends PluginRequest {

    private final SqlNode sqlNode;
    private final List<Datamart> schema;
    private final List<ColumnMetadata> metadata;
    private final QueryTemplateResult sourceQueryTemplateResult;

    public LlrRequest(QueryTemplateResult sourceQueryTemplateResult,
                      QueryRequest queryRequest,
                      List<Datamart> schema,
                      List<ColumnMetadata> metadata,
                      SqlNode sqlNode) {
        super(queryRequest);
        this.sourceQueryTemplateResult = sourceQueryTemplateResult;
        this.schema = schema;
        this.metadata = metadata;
        this.sqlNode = sqlNode;
    }

    @Override
    public String toString() {
        return "LlrRequest{" +
                "schema=" + schema +
                ", metadata=" + metadata +
                '}';
    }
}
