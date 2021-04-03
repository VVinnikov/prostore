package io.arenadata.dtm.query.execution.core.edml.mppw.dto;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.vertx.core.Future;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@Builder
public class MppwStopFuture {
    private SourceType sourceType;
    private Future<QueryResult> future;
    private Long offset;
    private Throwable cause;
    private MppwStopReason stopReason;
}
