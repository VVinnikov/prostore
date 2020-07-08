package ru.ibs.dtm.common.delta;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DeltaInformationResult {
    private final List<DeltaInformation> deltaInformations;
    private final String sqlWithoutSnapshots;
}
