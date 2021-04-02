package io.arenadata.dtm.query.execution.plugin.adqm.rollback.dto;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.PluginRollbackRequest;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
public class AdqmRollbackRequest extends PluginRollbackRequest {

    public AdqmRollbackRequest(List<PreparedStatementRequest> statements) {
        super(statements);
    }
}
