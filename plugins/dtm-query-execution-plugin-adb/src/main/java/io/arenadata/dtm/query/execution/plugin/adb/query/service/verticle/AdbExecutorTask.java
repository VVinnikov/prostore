package io.arenadata.dtm.query.execution.plugin.adb.query.service.verticle;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
public class AdbExecutorTask {
    private final String sql;
    private final QueryParameters params;
    private final List<ColumnMetadata> metadata;
    private final List<PreparedStatementRequest> preparedStatementRequests;
}
