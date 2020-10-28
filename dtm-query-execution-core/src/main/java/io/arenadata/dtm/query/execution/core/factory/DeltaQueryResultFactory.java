package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;

import java.time.format.DateTimeFormatter;

public interface DeltaQueryResultFactory {

    QueryResult create(DeltaRecord deltaQuery);

    QueryResult createEmpty();
}
