package ru.ibs.dtm.query.execution.plugin.api.edml;

import io.vertx.core.json.JsonObject;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.sql.SqlInsert;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.EDML;

@Getter
@Setter
@ToString
public class EdmlRequestContext extends RequestContext<DatamartRequest> {
    private final SqlInsert sqlNode;
    private JsonObject schema;
    private QueryExloadParam exloadParam;

    public EdmlRequestContext(DatamartRequest request, SqlInsert sqlNode) {
        super(request);
        this.sqlNode = sqlNode;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return EDML;
    }

}
