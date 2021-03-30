package io.arenadata.dtm.query.execution.plugin.adqm.dto.query;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.calcite.rel.RelNode;

import java.util.List;

@Data
@AllArgsConstructor
public class AdqmCheckJoinRequest {
    private final RelNode relNode;
    private List<Datamart> schema;
}
