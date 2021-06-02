package io.arenadata.dtm.query.execution.plugin.adb.rollback.dto;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.PluginRollbackRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class AdbRollbackRequest extends PluginRollbackRequest {
    private final PreparedStatementRequest truncate;
    private final PreparedStatementRequest deleteFromActual;
    private final List<PreparedStatementRequest> eraseOps;

    public AdbRollbackRequest(PreparedStatementRequest truncate,
                              PreparedStatementRequest deleteFromActual,
                              List<PreparedStatementRequest> eraseOps) {
        super(new ArrayList<>(Arrays.asList(
                truncate,
                deleteFromActual
        )));
        getStatements().addAll(eraseOps);

        this.truncate = truncate;
        this.deleteFromActual = deleteFromActual;
        this.eraseOps = eraseOps;
    }
}
