package io.arenadata.dtm.common.status;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StatusEventKey {
    private String datamart;
    private LocalDateTime datetime;
    private StatusEventCode event;
    private UUID eventLogId;
}
