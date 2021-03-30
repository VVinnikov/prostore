package io.arenadata.dtm.query.execution.core.service.query;

import io.arenadata.dtm.common.reader.QueryRequest;
import org.apache.calcite.sql.SqlNode;

public interface QueryPreparedService {

    SqlNode getPreparedQuery(QueryRequest request);
}
