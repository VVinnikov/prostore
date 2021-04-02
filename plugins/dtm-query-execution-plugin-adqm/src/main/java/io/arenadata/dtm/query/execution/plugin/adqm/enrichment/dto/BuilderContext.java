package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto;

import com.google.common.collect.Lists;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.exception.DtmException;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

@Data
@Builder
public class BuilderContext {
    private List<DeltaInformation> deltaInformations;
    private List<TableScan> tableScans;
    private RelNode lastChildNode;
    private List<RelBuilder> builders;

    public RelBuilder getBuilder() {
        RelBuilder relBuilder = builders.stream()
            .reduce((b1, b2) -> b2.push(b1.build()))
            .orElseThrow(() -> new DtmException("Can't get result relation builder"));
        builders = Lists.newArrayList(relBuilder);
        return relBuilder;
    }
}
