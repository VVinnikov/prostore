package io.arenadata.dtm.query.calcite.core.dto.delta;

import io.arenadata.dtm.common.delta.DeltaInformation;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class DeltaQueryPreprocessorResponse {
    private final List<DeltaInformation> deltaInformations;
    private final SqlNode sqlNode;
}
