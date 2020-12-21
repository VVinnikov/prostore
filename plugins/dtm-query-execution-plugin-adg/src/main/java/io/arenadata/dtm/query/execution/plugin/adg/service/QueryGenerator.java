package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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
                               QueryRequest queryRequest);
}
