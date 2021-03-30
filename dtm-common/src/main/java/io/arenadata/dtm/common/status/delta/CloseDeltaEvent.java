package io.arenadata.dtm.common.status.delta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CloseDeltaEvent {
    private LocalDateTime deltaDateTime;
}
