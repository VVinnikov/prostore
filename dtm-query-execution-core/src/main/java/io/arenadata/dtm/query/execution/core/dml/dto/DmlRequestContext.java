package io.arenadata.dtm.query.execution.core.dml.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlUseSchema;
import io.arenadata.dtm.query.execution.core.base.dto.CoreRequestContext;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import static io.arenadata.dtm.common.model.SqlProcessingType.DML;

@Getter
@Setter
@ToString
public class DmlRequestContext extends CoreRequestContext<DmlRequest, SqlNode> {

    private final SourceType sourceType;
    private final DmlType type;

    @Builder
    protected DmlRequestContext(RequestMetrics metrics,
                                String envName,
                                DmlRequest request,
                                SqlNode sqlNode,
                                SourceType sourceType) {
        super(metrics, envName, request, sqlNode);
        this.sourceType = sourceType;
        type = sqlNode instanceof SqlUseSchema ? DmlType.USE : DmlType.LLR;
    }


    @Override
    public SqlProcessingType getProcessingType() {
        return DML;
    }
}
