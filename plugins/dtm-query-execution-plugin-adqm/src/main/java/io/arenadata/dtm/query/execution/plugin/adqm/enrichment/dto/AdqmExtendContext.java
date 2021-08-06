package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto;

import io.arenadata.dtm.common.delta.DeltaInformation;
import lombok.Data;
import org.apache.calcite.rel.core.TableScan;

import java.util.ArrayList;
import java.util.List;

@Data
public class AdqmExtendContext {
    private final List<TableScan> tableScans = new ArrayList<>();
    private final List<DeltaInformation> deltasToAdd = new ArrayList<>();
}
