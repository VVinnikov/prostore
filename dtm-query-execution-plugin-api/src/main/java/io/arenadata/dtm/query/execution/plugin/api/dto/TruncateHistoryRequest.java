package io.arenadata.dtm.query.execution.plugin.api.dto;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

import java.util.Optional;

@Getter
public class TruncateHistoryRequest extends PluginRequest {
    private final Optional<Long> sysCn;
    private final Entity entity;
    private final String env;
    private final Optional<SqlNode> conditions;

    public TruncateHistoryRequest(Long sysCn,
                                  Entity entity,
                                  String env,
                                  SqlNode conditions) {
        this.sysCn = Optional.ofNullable(sysCn);
        this.entity = entity;
        this.env = env;
        this.conditions = Optional.ofNullable(conditions);
    }
}
