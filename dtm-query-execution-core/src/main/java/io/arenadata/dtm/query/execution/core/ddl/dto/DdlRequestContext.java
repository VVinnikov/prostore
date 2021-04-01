package io.arenadata.dtm.query.execution.core.ddl.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.core.base.dto.CoreRequestContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

import static io.arenadata.dtm.common.model.SqlProcessingType.DDL;
import static io.arenadata.dtm.query.execution.core.ddl.dto.DdlType.UNKNOWN;

@Getter
@Setter
public class DdlRequestContext extends CoreRequestContext<DatamartRequest, SqlNode> {

    private DdlType ddlType;
    private SqlCall sqlCall;
    private String datamartName;
    private Entity entity;
    private SourceType sourceType;
    private List<PostSqlActionType> postActions;

    public DdlRequestContext(RequestMetrics metrics,
                             DatamartRequest request,
                             SqlNode query,
                             SourceType sourceType,
                             String envName) {
        super(metrics, envName, request, query);
        this.ddlType = UNKNOWN;
        this.sourceType = sourceType;
        this.postActions = new ArrayList<>();
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DDL;
    }

}
