package ru.ibs.dtm.common.status.delta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CloseDeltaEvent {
    private long deltaNum;
    private LocalDateTime deltaDateTime;
}
