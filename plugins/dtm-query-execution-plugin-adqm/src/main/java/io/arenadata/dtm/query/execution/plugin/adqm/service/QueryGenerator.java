package io.arenadata.dtm.query.execution.plugin.adqm.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.vertx.core.Future;
import org.apache.calcite.rel.RelRoot;

import java.util.List;

/**
 * DML query converters
 */
public interface QueryGenerator {
    /**
     * Convert Query
     */
    Future<String> mutateQuery(RelRoot sqlNode,
                               List<DeltaInformation> deltaInformations,
                               CalciteContext calciteContext,
                               QueryRequest queryRequest,
                               boolean isLocal);
}
