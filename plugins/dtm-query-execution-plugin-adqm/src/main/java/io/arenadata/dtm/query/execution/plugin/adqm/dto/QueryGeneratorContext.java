package io.arenadata.dtm.query.execution.plugin.adqm.dto;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QueryRequest;
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
    private final QueryRequest queryRequest;
    private final RelBuilder relBuilder;
    private final RelRoot relNode;
}