package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import io.vertx.core.Future;
import org.apache.calcite.rel.RelRoot;

import java.util.List;

/**
 * Dml query modifier
 */
public interface QueryGenerator {
    /**
     * modify query
     */
    Future<String> mutateQuery(RelRoot sqlNode,
                               List<DeltaInformation> deltaInformations,
                               CalciteContext calciteContext,
                               EnrichQueryRequest enrichQueryRequest);
}
