package ru.ibs.dtm.query.execution.plugin.adqm.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.rel.RelRoot;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.reader.QueryRequest;

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
