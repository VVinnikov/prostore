package ru.ibs.dtm.query.execution.core.dto.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;

import java.time.LocalDateTime;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
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
