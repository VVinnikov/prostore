package ru.ibs.dtm.query.execution.plugin.api.edml;

import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.EDML;

@Getter
@ToString
public class EdmlRequestContext extends RequestContext<DatamartRequest> {
    private final SqlInsert sqlNode;

    public EdmlRequestContext(DatamartRequest request, SqlInsert sqlNode) {
        super(request);
        this.sqlNode = sqlNode;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return EDML;
    }

}