package io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
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
    Future<String> mutateQuery(RelRoot relNode,
                               List<DeltaInformation> deltaInformations,
                               CalciteContext calciteContext,
                               EnrichQueryRequest enrichQueryRequest);
}
