package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto;

import io.arenadata.dtm.common.delta.DeltaInformation;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.rel.RelNode;

@Data
@Builder
public class RelNodeContext {
    private DeltaInformation deltaInformation;
    private RelNode parent;
    private RelNode child;
    private int childCount;
    private int depth;
    private int i;
}
