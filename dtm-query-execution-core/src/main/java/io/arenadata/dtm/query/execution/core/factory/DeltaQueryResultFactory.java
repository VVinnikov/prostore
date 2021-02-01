package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;

public interface DeltaQueryResultFactory {

    QueryResult create(DeltaRecord deltaQuery);

    QueryResult createEmpty();
}
