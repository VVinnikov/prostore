package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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
