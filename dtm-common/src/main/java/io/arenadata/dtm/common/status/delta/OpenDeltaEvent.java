package io.arenadata.dtm.common.status.delta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OpenDeltaEvent {
    private long deltaNum;
}
