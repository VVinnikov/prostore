package io.arenadata.dtm.query.execution.plugin.adb.base.dto;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EnrichQueryRequest {
    private List<DeltaInformation> deltaInformations;
    private List<Datamart> schema;
    private String envName;
    private SqlNode query;
}
