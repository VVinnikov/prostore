package io.arenadata.dtm.query.execution.plugin.api.dml;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlUseSchema;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import static io.arenadata.dtm.common.model.SqlProcessingType.DML;

@Getter
@ToString
public class DmlRequestContext extends RequestContext<DatamartRequest> {

    private final SqlNode query;
    private final DmlType type;

    public DmlRequestContext(RequestMetrics metrics,
                             DatamartRequest request,
                             SqlNode query) {
        super(metrics, request);
        this.query = query;
        type = query instanceof SqlUseSchema ? DmlType.USE : DmlType.LLR;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DML;
    }
}
