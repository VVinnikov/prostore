package io.arenadata.dtm.query.execution.core.delta.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.arenadata.dtm.common.delta.DeltaLoadStatus;
import io.arenadata.dtm.query.execution.core.delta.dto.operation.WriteOpFinish;
import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class DeltaRecord {
    private String datamart;
    private LocalDateTime deltaDate;
    private Long deltaNum;
    private Long cnFrom;
    private Long cnTo;
    private Long cnMax;
    private boolean rollingBack;
    private List<WriteOpFinish> writeOperationsFinished;
    private DeltaLoadStatus status;
}
