package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import lombok.Data;

import java.util.List;

@Data
public class AdbKafkaMppwTransferRequest {
    private final List<PreparedStatementRequest> firstTransaction;
    private final List<PreparedStatementRequest> secondTransaction;
}
