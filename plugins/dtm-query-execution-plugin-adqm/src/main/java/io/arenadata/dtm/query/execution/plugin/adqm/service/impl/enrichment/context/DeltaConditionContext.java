package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.enrichment.context;

import io.arenadata.dtm.common.delta.DeltaInformation;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.tools.RelBuilder;

@Data
@Builder
public class DeltaConditionContext {
    private DeltaInformation deltaInfo;
    private RelBuilder builder;
    private boolean finalize;
    private int tableCount;
}
