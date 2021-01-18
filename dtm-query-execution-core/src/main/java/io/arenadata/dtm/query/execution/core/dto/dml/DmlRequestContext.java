package io.arenadata.dtm.query.execution.core.dto.dml;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlUseSchema;
import io.arenadata.dtm.query.execution.plugin.api.CoreRequestContext;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import static io.arenadata.dtm.common.model.SqlProcessingType.DML;

@Getter
@Setter
@ToString
public class DmlRequestContext extends CoreRequestContext<DatamartRequest, SqlNode> {

    private final DmlType type;
    private final SourceType sourceType;

    public DmlRequestContext(DatamartRequest request,
                             String envName,
                             SqlNode sqlNode,
                             SourceType sourceType) {
        super(request, envName, sqlNode, metrics);
        type = sqlNode instanceof SqlUseSchema ? DmlType.USE : DmlType.LLR;
        this.sourceType = sourceType;
    }

    public DmlRequestContext(DatamartRequest request,
                             String envName,
                             RequestMetrics metrics,
                             SqlNode sqlNode,
                             SourceType sourceType) {
        super(request, envName, metrics, sqlNode);
        type = sqlNode instanceof SqlUseSchema ? DmlType.USE : DmlType.LLR;
        this.sourceType = sourceType;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DML;
    }
}
