package ru.ibs.dtm.query.execution.plugin.adb.dto;

import java.util.Iterator;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.tools.RelBuilder;
import ru.ibs.dtm.common.delta.DeltaInformation;

@Data
@AllArgsConstructor
public class QueryGeneratorContext {
    private final Iterator<DeltaInformation> deltaIterator;
    private final RelBuilder relBuilder;
    private final boolean clearOptions;
    private final RelRoot relNode;
}