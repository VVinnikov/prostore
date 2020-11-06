package io.arenadata.dtm.query.execution.plugin.api.ddl;

import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import lombok.Data;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

import static io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType.UNKNOWN;
import static io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType.DDL;

@Data
@ToString
public class DdlRequestContext extends RequestContext<DdlRequest> {

    private DdlType ddlType;
    private String datamartName;
    private SqlNode query;
    private String systemName;
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

    @Override
    public SqlProcessingType getProcessingType() {
        return DDL;
    }

}
