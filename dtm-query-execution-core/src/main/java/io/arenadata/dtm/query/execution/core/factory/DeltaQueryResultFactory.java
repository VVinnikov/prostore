package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;

public interface DeltaQueryResultFactory {

    QueryResult create(DeltaRequestContext context, DeltaRecord deltaRecord);
}
