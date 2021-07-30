package io.arenadata.dtm.query.execution.plugin.adp.rollback.factory;

import io.arenadata.dtm.common.model.ddl.Entity;

public class AdpRollbackSqlFactory {
    private AdpRollbackSqlFactory() {
    }

    private static final String ROLLBACK_SQL_TEMPLATE = "TRUNCATE ${datamart}.${tableName}_staging;\n" +
            "DELETE FROM ${datamart}.${tableName}_actual WHERE sys_from = ${sysCn};\n" +
            "UPDATE ${datamart}.${tableName}_actual SET sys_to = NULL, sys_op = 0 WHERE sys_to = ${previousSysCn};";

    public static String getRollbackSql(String datamart, Entity entity, Long sysCn) {
        String currentSysCn = Long.toString(sysCn);
        String previousSysCn = Long.toString(sysCn - 1L);

        return ROLLBACK_SQL_TEMPLATE.replace("${datamart}", datamart)
                .replace("${tableName}", entity.getName())
                .replace("${sysCn}", currentSysCn)
                .replace("${previousSysCn}", previousSysCn);
    }

}
