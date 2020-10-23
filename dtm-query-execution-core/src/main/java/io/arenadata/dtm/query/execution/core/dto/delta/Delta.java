package io.arenadata.dtm.query.execution.core.dto.delta;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class Delta {
    private HotDelta hot;
    private OkDelta ok;
}
