package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.edml.EraseWriteOpResult;

import java.util.List;

public interface RollbackWriteOpsQueryResultFactory {

    QueryResult create(List<EraseWriteOpResult> eraseOps);
}
