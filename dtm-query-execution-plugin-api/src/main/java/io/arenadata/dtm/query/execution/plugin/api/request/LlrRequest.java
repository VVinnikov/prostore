package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
public class LlrRequest extends PluginRequest {

    private final QueryTemplateResult sourceQueryTemplateResult;
    private final List<DeltaInformation> deltaInformations;
    private final List<ColumnMetadata> metadata;
    private final QueryParameters parameters;
    private final List<Datamart> schema;
    private final SqlNode sqlNode;

    @Builder(toBuilder = true)
    public LlrRequest(UUID requestId,
                      String envName,
                      String datamartMnemonic,
                      SqlNode sqlNode,
                      List<Datamart> schema,
                      List<ColumnMetadata> metadata,
                      QueryTemplateResult sourceQueryTemplateResult,
                      List<DeltaInformation> deltaInformations,
                      QueryParameters parameters) {
        super(requestId, envName, datamartMnemonic);
        this.sqlNode = sqlNode;
        this.schema = schema;
        this.metadata = metadata;
        this.sourceQueryTemplateResult = sourceQueryTemplateResult;
        this.deltaInformations = deltaInformations;
        this.parameters = parameters;
    }
}
