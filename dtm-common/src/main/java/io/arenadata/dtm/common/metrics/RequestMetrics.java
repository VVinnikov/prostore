package io.arenadata.dtm.common.metrics;

import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RequestMetrics {
    private UUID requestId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private SqlProcessingType actionType;
    private SourceType sourceType;
    private RequestStatus status;
    private boolean isActive;
}
