package io.arenadata.dtm.query.execution.plugin.api.edml;

import io.arenadata.dtm.common.dto.TableInfo;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.sql.SqlInsert;

import java.util.List;

import static io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType.EDML;

@Getter
@Setter
@ToString
public class EdmlRequestContext extends RequestContext<DatamartRequest> {
    private Entity sourceEntity;
    private Entity destinationEntity;
    private Long sysCn;
    private TableInfo sourceTable;
    private TableInfo destinationTable;
    private final SqlInsert sqlNode;
    private String dmlSubquery;
    private List<Datamart> logicalSchema;

    public EdmlRequestContext(DatamartRequest request, SqlInsert sqlNode) {
        super(request);
        this.sqlNode = sqlNode;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return EDML;
    }

}
