package io.arenadata.dtm.query.execution.plugin.adg.dto;

import io.arenadata.dtm.common.delta.DeltaInformation;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.tools.RelBuilder;

import java.util.Iterator;

@Data
@AllArgsConstructor
public class QueryGeneratorContext {
    private final Iterator<DeltaInformation> deltaIterator;
    private final EnrichQueryRequest enrichQueryRequest;
    private final RelBuilder relBuilder;
    private final boolean clearOptions;
    private final RelRoot relNode;
}
