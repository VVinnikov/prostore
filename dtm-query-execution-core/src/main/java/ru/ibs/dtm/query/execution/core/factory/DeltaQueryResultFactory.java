package ru.ibs.dtm.query.execution.core.factory;

import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;

public interface DeltaQueryResultFactory {

    QueryResult create(DeltaRequestContext context, DeltaRecord deltaRecord);
}
