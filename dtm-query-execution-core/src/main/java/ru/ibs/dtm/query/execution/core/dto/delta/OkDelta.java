package ru.ibs.dtm.query.execution.core.dto.delta;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OkDelta {
    private long deltaNum;
    private LocalDateTime deltaDate;
    private long cnFrom;
    private long cnTo;
}
