package io.arenadata.dtm.query.execution.plugin.api.synchronize;

import io.arenadata.dtm.common.delta.DeltaData;
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
    private final DeltaData deltaToBe;
    private final Long beforeDeltaCnTo;

    public SynchronizeRequest(UUID requestId,
                              String envName,
                              String datamartMnemonic,
                              List<Datamart> datamarts,
                              Entity entity,
                              SqlNode viewQuery,
                              DeltaData deltaToBe,
                              Long beforeDeltaCnTo) {
        super(requestId, envName, datamartMnemonic);
        this.datamarts = datamarts;
        this.viewQuery = viewQuery;
        this.entity = entity;
        this.deltaToBe = deltaToBe;
        this.beforeDeltaCnTo = beforeDeltaCnTo;
    }
}
