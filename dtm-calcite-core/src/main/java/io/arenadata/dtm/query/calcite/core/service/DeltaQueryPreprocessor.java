package io.arenadata.dtm.query.calcite.core.service;

import io.arenadata.dtm.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

public interface DeltaQueryPreprocessor {
    Future<DeltaQueryPreprocessorResponse> process(SqlNode request);
}
