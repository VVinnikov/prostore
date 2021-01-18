package io.arenadata.dtm.query.execution.plugin.api.ddl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.CoreRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import lombok.Data;
import lombok.ToString;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

import static io.arenadata.dtm.common.model.SqlProcessingType.DDL;
import static io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType.UNKNOWN;

@Data
@ToString
public class DdlRequestContext extends CoreRequestContext<DdlRequest, SqlNode> {

    private DdlType ddlType;
    private SqlCall sqlCall;
    private String datamartName;
    private String systemName;
    private SourceType sourceType;
    private List<PostSqlActionType> postActions;

    public DdlRequestContext(final DdlRequest request) {
        this(request, null);
    }

    public DdlRequestContext(final DdlRequest request, final SqlNode query) {
        super(request);
        this.ddlType = UNKNOWN;
        this.query = query;
        this.systemName = "local";
        this.postActions = new ArrayList<>();
    }

    public DdlRequestContext(RequestMetrics metrics,
                             DdlRequest request,
                             SqlNode query,
                             SourceType sourceType,
                             String envName) {
        super(request, query, envName, metrics);
        this.ddlType = UNKNOWN;
        this.systemName = "local";
        this.sourceType = sourceType;
        this.postActions = new ArrayList<>();
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DDL;
    }

}
