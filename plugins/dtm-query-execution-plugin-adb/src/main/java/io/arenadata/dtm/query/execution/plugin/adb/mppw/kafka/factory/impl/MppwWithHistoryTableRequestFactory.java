package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.impl;

public class MppwWithHistoryTableRequestFactory extends AbstractMppwRequestFactory {

    private static final String INSERT_HISTORY_SQL = "INSERT INTO %s.%s_history (%s)\n" +
        "SELECT %s\n" +
        "FROM %s.%s_actual a\n" +
        "         INNER JOIN (SELECT DISTINCT * FROM %s.%s_staging) s ON\n" +
        "    %s";

    private static final String DELETE_ACTUAL_SQL = "DELETE\n" +
        "FROM %s.%s_actual a USING %s.%s_staging s\n" +
        "WHERE %s";

    private static final String INSERT_ACTUAL_SQL = "INSERT INTO %s.%s_actual (%s)\n" +
        "SELECT DISTINCT %s\n" +
        "FROM %s.%s_staging\n" +
        "WHERE %s.%s_staging.sys_op <> 1";

    private static final String TRUNCATE_STAGING_SQL = "TRUNCATE %s.%s_staging";

    @Override
    protected String getTruncateStagingSql() {
        return TRUNCATE_STAGING_SQL;
    }

    @Override
    protected String getInsertActualSql() {
        return INSERT_ACTUAL_SQL;
    }

    @Override
    protected String getDeleteActualSql() {
        return DELETE_ACTUAL_SQL;
    }

    @Override
    protected String getInsertHistorySql() {
        return INSERT_HISTORY_SQL;
    }
}
