package io.arenadata.dtm.query.execution.core.dto.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.arenadata.dtm.common.delta.DeltaLoadStatus;
import lombok.*;

import java.time.LocalDateTime;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class DeltaRecord {

    private Long loadId;
    private String datamartMnemonic;
    private LocalDateTime sysDate;
    private LocalDateTime statusDate;
    private Long sinId;
    private String loadProcId;
    private DeltaLoadStatus status;
}
