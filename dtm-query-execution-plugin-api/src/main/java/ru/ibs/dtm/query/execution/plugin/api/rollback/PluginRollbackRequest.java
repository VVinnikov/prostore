package ru.ibs.dtm.query.execution.plugin.api.rollback;

import lombok.Data;
import ru.ibs.dtm.common.plugin.sql.PreparedStatementRequest;

import java.util.List;

@Data
public class PluginRollbackRequest {
    private final List<PreparedStatementRequest> statements;

    public PluginRollbackRequest(List<PreparedStatementRequest> statements) {
        this.statements = statements;
    }
}
