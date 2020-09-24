package ru.ibs.dtm.query.execution.plugin.adqm.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.tools.RelBuilder;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.reader.QueryRequest;

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
