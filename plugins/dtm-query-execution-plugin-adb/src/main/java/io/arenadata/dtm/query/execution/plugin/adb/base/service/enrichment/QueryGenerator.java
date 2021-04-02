package io.arenadata.dtm.query.execution.plugin.adb.base.service.enrichment;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.vertx.core.Future;
import org.apache.calcite.rel.RelRoot;

import java.util.List;

/**
 * Query generator
 */
public interface QueryGenerator {
    /**
     * mutate query with extending
     */
    Future<String> mutateQuery(RelRoot sqlNode,
                               List<DeltaInformation> deltaInformations,
                               CalciteContext calciteContext);
}
