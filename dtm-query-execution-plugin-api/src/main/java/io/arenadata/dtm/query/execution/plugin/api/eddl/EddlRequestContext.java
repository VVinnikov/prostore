package io.arenadata.dtm.query.execution.plugin.api.eddl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.CoreRequestContext;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import static io.arenadata.dtm.common.model.SqlProcessingType.EDDL;

@ToString
public class EddlRequestContext extends CoreRequestContext<DatamartRequest, SqlNode> {

    public EddlRequestContext(RequestMetrics metrics,
                              DatamartRequest request,
                              String envName,
                              SqlNode sqlNode) {
        super(metrics, envName, request, sqlNode);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return EDDL;
    }
}
