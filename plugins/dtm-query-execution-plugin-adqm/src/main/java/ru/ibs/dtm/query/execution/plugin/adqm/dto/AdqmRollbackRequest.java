package ru.ibs.dtm.query.execution.plugin.adqm.dto;

import lombok.EqualsAndHashCode;
import ru.ibs.dtm.common.plugin.sql.PreparedStatementRequest;
import ru.ibs.dtm.query.execution.plugin.api.rollback.PluginRollbackRequest;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
public class AdqmRollbackRequest extends PluginRollbackRequest {

    public AdqmRollbackRequest(List<PreparedStatementRequest> statements) {
        super(statements);
    }
}
