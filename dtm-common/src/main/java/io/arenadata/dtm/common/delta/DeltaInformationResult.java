package io.arenadata.dtm.common.delta;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
@AllArgsConstructor
public class DeltaInformationResult {
    private final List<DeltaInformation> deltaInformations;
    private final SqlNode sqlWithoutSnapshots;
}
