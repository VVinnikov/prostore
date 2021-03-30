package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.vertx.core.Future;
import org.apache.calcite.rel.RelRoot;

/**
 * Query parsing service
 */
public interface QueryParserService {
    Future<RelRoot> parse(QueryRequest querySourceRequest, CalciteContext calciteContext);
}
