package io.arenadata.dtm.query.execution.plugin.adqm.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.rel.RelRoot;

import java.util.List;

/**
 * DML query converters
 */
public interface QueryGenerator {
    /**
     * Convert Query
     */
    void mutateQuery(RelRoot sqlNode,
                     List<DeltaInformation> deltaInformations,
                     CalciteContext calciteContext,
                     QueryRequest queryRequest,
                     Handler<AsyncResult<String>> handler);
}
