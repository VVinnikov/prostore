package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class EnrichQueryRequest {
    private List<DeltaInformation> deltaInformations;
    private List<Datamart> schema;
    private String envName;
    private SqlNode query;
    @Builder.Default
    private boolean isLocal;

    @Builder
    public EnrichQueryRequest(List<DeltaInformation> deltaInformations,
                              List<Datamart> schema,
                              boolean isLocal,
                              String envName,
                              SqlNode query) {
        this.deltaInformations = deltaInformations;
        this.schema = schema;
        this.isLocal = isLocal;
        this.envName = envName;
        this.query = query;
    }
}
