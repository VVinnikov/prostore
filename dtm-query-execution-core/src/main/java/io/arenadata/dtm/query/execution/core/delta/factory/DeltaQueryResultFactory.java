package io.arenadata.dtm.query.execution.core.delta.factory;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaRecord;

public interface DeltaQueryResultFactory {

    QueryResult create(DeltaRecord deltaQuery);

    QueryResult createEmpty();
}
