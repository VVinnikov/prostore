package io.arenadata.dtm.query.execution.core.rollback.factory;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.edml.dto.EraseWriteOpResult;

import java.util.List;

public interface RollbackWriteOpsQueryResultFactory {

    QueryResult create(List<EraseWriteOpResult> eraseOps);
}
