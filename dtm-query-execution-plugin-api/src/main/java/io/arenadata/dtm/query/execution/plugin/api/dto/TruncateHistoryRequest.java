package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.Builder;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

import java.util.Optional;
import java.util.UUID;

@Getter
public class TruncateHistoryRequest extends PluginRequest {
    private final Optional<Long> sysCn;
    private final Entity entity;
    private final Optional<SqlNode> conditions;

    @Builder
    public TruncateHistoryRequest(UUID requestId, String envName, String datamartMnemonic, Long sysCn, Entity entity, SqlNode conditions) {
        super(requestId, envName, datamartMnemonic);
        this.sysCn = Optional.ofNullable(sysCn);
        this.entity = entity;
        this.conditions = Optional.ofNullable(conditions);
    }
}
