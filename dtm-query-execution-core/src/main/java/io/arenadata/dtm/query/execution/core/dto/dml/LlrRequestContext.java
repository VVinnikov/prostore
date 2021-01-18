package io.arenadata.dtm.query.execution.core.dto.dml;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
public class LlrRequestContext {
    private List<DeltaInformation> deltaInformations;
    private DmlRequestContext dmlRequestContext;
    private QuerySourceRequest sourceRequest;

    @Builder
    public LlrRequestContext(List<DeltaInformation> deltaInformations,
                             DmlRequestContext dmlRequestContext,
                             QuerySourceRequest sourceRequest) {
        this.deltaInformations = deltaInformations;
        this.dmlRequestContext = dmlRequestContext;
        this.sourceRequest = sourceRequest;
    }
}
