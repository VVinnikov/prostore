package io.arenadata.dtm.query.execution.plugin.api.synchronize;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.UUID;

@Getter
@ToString
public class SynchronizeRequest extends PluginRequest {
    private final List<Datamart> datamarts;
    private final Entity entity;
    private final SqlNode viewQuery;
    private final Long deltaNumToBe;
    private final Long deltaNumToBeCnTo;

    public SynchronizeRequest(UUID requestId,
                              String envName,
                              String datamartMnemonic,
                              List<Datamart> datamarts,
                              Entity entity,
                              SqlNode viewQuery,
                              Long deltaNumToBe,
                              Long deltaNumToBeCnTo) {
        super(requestId, envName, datamartMnemonic);
        this.datamarts = datamarts;
        this.viewQuery = viewQuery;
        this.deltaNumToBe = deltaNumToBe;
        this.entity = entity;
        this.deltaNumToBeCnTo = deltaNumToBeCnTo;
    }
}
