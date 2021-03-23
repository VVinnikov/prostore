package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.query.execution.core.dto.dml.LlrRequestContext;
import io.vertx.core.Future;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public interface ParametersTypeExtractor {
    Future<List<SqlTypeName>> extract(LlrRequestContext llrRequestContext);
}
