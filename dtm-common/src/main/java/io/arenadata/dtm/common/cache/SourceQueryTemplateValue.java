package io.arenadata.dtm.common.cache;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class SourceQueryTemplateValue {
    private String sql;
    private List<DeltaInformation> deltaInformations;
    private List<ColumnMetadata> metadata;
    private List<Datamart> logicalSchema;
}
