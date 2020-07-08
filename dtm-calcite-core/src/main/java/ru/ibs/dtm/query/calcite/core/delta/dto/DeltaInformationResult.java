package ru.ibs.dtm.query.calcite.core.delta.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DeltaInformationResult {
    private List<DeltaInformation> deltaInformations;
    private String sqlWithoutSnapshots;
}
