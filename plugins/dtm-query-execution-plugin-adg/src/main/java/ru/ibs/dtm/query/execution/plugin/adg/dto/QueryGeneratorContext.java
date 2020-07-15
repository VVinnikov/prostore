package ru.ibs.dtm.query.execution.plugin.adg.dto;

import java.util.Iterator;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.tools.RelBuilder;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.reader.QueryRequest;

@Data
@AllArgsConstructor
public class QueryGeneratorContext {
    private final Iterator<DeltaInformation> deltaIterator;
    private final QueryRequest queryRequest;
    private final RelBuilder relBuilder;
    private final boolean clearOptions;
    private final RelRoot relNode;
}
