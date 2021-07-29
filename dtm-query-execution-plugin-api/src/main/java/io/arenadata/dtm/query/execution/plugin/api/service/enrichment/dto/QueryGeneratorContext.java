package io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto;

import io.arenadata.dtm.common.delta.DeltaInformation;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.tools.RelBuilder;

import java.util.Iterator;

@Data
@Builder
@AllArgsConstructor
public class QueryGeneratorContext {
    private final Iterator<DeltaInformation> deltaIterator;
    private final RelBuilder relBuilder;
    private final RelRoot relNode;
    private final boolean clearOptions;
    private EnrichQueryRequest enrichQueryRequest;

    public QueryGeneratorContext(Iterator<DeltaInformation> deltaIterator, RelBuilder relBuilder, RelRoot relNode, boolean clearOptions) {
        this.deltaIterator = deltaIterator;
        this.relBuilder = relBuilder;
        this.relNode = relNode;
        this.clearOptions = clearOptions;
    }
}
