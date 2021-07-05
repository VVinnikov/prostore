package io.arenadata.dtm.common.delta;

import lombok.Data;

@Data
public class DeltaData {
    private final long num;
    private final long cnFrom;
    private final long cnTo;
}
