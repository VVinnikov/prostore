package io.arenadata.dtm.query.execution.plugin.adb.dto;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.PluginRollbackRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Arrays;

@Data
@EqualsAndHashCode(callSuper = true)
public class AdbRollbackRequest extends PluginRollbackRequest {
    private final PreparedStatementRequest deleteFromHistory;
    private final PreparedStatementRequest deleteFromActual;
    private final PreparedStatementRequest truncate;
    private final PreparedStatementRequest insert;

    public AdbRollbackRequest(PreparedStatementRequest deleteFromHistory,
                              PreparedStatementRequest deleteFromActual,
                              PreparedStatementRequest truncate,
                              PreparedStatementRequest insert) {
        super(Arrays.asList(
            truncate,
            deleteFromActual,
            insert,
            deleteFromHistory
        ));
        this.deleteFromHistory = deleteFromHistory;
        this.deleteFromActual = deleteFromActual;
        this.truncate = truncate;
        this.insert = insert;
    }
}
