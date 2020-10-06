package ru.ibs.dtm.query.execution.core.dto.delta;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HotDelta {
    private long deltaNum;
    private long cnFrom;
    private Long cnTo;
    private long cnMax;
    private boolean rollingBack;
}
