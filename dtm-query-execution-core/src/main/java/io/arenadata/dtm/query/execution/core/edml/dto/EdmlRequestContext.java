package io.arenadata.dtm.query.execution.core.edml.dto;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.core.base.dto.CoreRequestContext;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

import static io.arenadata.dtm.common.model.SqlProcessingType.EDML;

@Getter
@Setter
@ToString
public class EdmlRequestContext extends CoreRequestContext<DatamartRequest, SqlNode> {
    private Entity sourceEntity;
    private Entity destinationEntity;
    private Long sysCn;
    private SqlNode dmlSubQuery;
    private List<Datamart> logicalSchema;
    private List<DeltaInformation> deltaInformations;

    public EdmlRequestContext(RequestMetrics metrics,
                              DatamartRequest request,
                              SqlNode sqlNode,
                              String envName) {
        super(metrics, envName, request, sqlNode);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return EDML;
    }

}
