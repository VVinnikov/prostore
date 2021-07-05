package io.arenadata.dtm.query.execution.plugin.adb.synchronize.factory;

import io.arenadata.dtm.common.model.ddl.Entity;

public interface SynchronizeSqlFactory {
    String createExternalTable(String env, String datamart, Entity matView);

    String dropExternalTable(String datamart, Entity matView);

    String insertIntoExternalTable(String datamart, Entity matView, String query, boolean onlyPrimaryKeys);
}
