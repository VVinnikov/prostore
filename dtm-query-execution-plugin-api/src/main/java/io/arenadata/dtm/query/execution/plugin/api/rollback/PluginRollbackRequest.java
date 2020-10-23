package io.arenadata.dtm.query.execution.plugin.api.rollback;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import lombok.Data;

import java.util.List;

@Data
public class PluginRollbackRequest {
    private final List<PreparedStatementRequest> statements;

    public PluginRollbackRequest(List<PreparedStatementRequest> statements) {
        this.statements = statements;
    }
}
